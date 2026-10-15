package consul

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/jpillora/backoff"
	"github.com/seatgeek/resec/resec/state"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type fakeClient struct {
	sync.Mutex
	registered   []*consulapi.AgentServiceRegistration
	deregistered []string
	ttlUpdates   []string
	registerErr  error
	nodeName     string
	nodeNameErr  error
	health       func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error)
}

func (c *fakeClient) LockOpts(opts *consulapi.LockOptions) (*consulapi.Lock, error) {
	return nil, nil
}

func (c *fakeClient) ServiceRegister(service *consulapi.AgentServiceRegistration) error {
	c.Lock()
	defer c.Unlock()
	if c.registerErr != nil {
		return c.registerErr
	}
	c.registered = append(c.registered, service)
	return nil
}

func (c *fakeClient) CheckRegister(check *consulapi.AgentCheckRegistration) error {
	return nil
}

func (c *fakeClient) ServiceDeregister(serviceID string) error {
	c.Lock()
	defer c.Unlock()
	c.deregistered = append(c.deregistered, serviceID)
	return nil
}

func (c *fakeClient) UpdateTTL(checkID, output, status string) error {
	c.Lock()
	defer c.Unlock()
	c.ttlUpdates = append(c.ttlUpdates, checkID)
	return nil
}

func (c *fakeClient) ServiceHealth(service, tag string, passingOnly bool, q *consulapi.QueryOptions) ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
	return c.health()
}

func (c *fakeClient) NodeName() (string, error) {
	return c.nodeName, c.nodeNameErr
}

func newTestManager(client *fakeClient) *Manager {
	return &Manager{
		backoff: &backoff.Backoff{Min: time.Millisecond, Max: 10 * time.Millisecond},
		client:  client,
		config: &config{
			announceAddr:           "10.0.0.1:6379",
			announceHost:           "10.0.0.1",
			announcePort:           6379,
			deregisterServiceAfter: time.Minute,
			serviceName:            "resec-redis",
			serviceTTL:             time.Second,
			serviceTagsByRole: map[string][]string{
				"master": {"master"},
				"slave":  {"readonly"},
			},
		},
		logger:    log.WithField("testing", true),
		stateCh:   make(chan state.Consul, 100),
		commandCh: make(chan Command, 10),
		stopCh:    make(chan interface{}),
		state:     &state.Consul{Ready: true, Healthy: true},
	}
}

func serviceEntry(address string, port int) *consulapi.ServiceEntry {
	return &consulapi.ServiceEntry{
		Node:    &consulapi.Node{Address: address},
		Service: &consulapi.AgentService{Address: address, Port: port},
	}
}

func portOnlyEntry(node, nodeAddress string, port int) *consulapi.ServiceEntry {
	return &consulapi.ServiceEntry{
		Node:    &consulapi.Node{Node: node, Address: nodeAddress},
		Service: &consulapi.AgentService{ID: "resec-redis@:" + strconv.Itoa(port), Port: port},
	}
}

// waitForMaster reads emitted states until one names the expected master
func waitForMaster(t *testing.T, m *Manager, address string, port int) {
	timeout := time.After(2 * time.Second)

	for {
		select {
		case s := <-m.stateCh:
			if s.MasterAddr == address && s.MasterPort == port {
				return
			}
		case <-timeout:
			t.Fatalf("master never became %s:%d", address, port)
		}
	}
}

func TestManager_StepDownForgetsSelfAsMaster(t *testing.T) {
	client := &fakeClient{}
	m := newTestManager(client)
	m.state.Master = true
	m.state.MasterAddr = "10.0.0.1"
	m.state.MasterPort = 6379
	m.registerService(state.Redis{})

	m.stepDown()

	assert.False(t, m.state.Master)
	assert.True(t, m.state.NoMasterElected())
	assert.Equal(t, Command{name: DeregisterServiceCommand}, <-m.commandCh)

	emitted := <-m.stateCh
	assert.False(t, emitted.Master)
	assert.True(t, emitted.NoMasterElected())
}

func TestManager_StepDownWhileWatcherSeesOwnEntry(t *testing.T) {
	client := &fakeClient{
		health: func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
			return []*consulapi.ServiceEntry{serviceEntry("10.0.0.1", 6379)}, &consulapi.QueryMeta{LastIndex: 5}, nil
		},
	}
	m := newTestManager(client)
	m.state.Master = true
	defer close(m.stopCh)

	go m.watchConsulMasterService()
	waitForMaster(t, m, "10.0.0.1", 6379)

	m.stepDown()

	// keep polling; our own entry must never become the master again
	time.Sleep(600 * time.Millisecond)
	m.masterLock.Lock()
	defer m.masterLock.Unlock()
	assert.True(t, m.state.NoMasterElected())
}

func TestManager_UpdateServiceReRegistersAfterRoleChange(t *testing.T) {
	client := &fakeClient{}
	m := newTestManager(client)

	m.state.Master = true
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.registered, 1)
	assert.Equal(t, []string{"master"}, client.registered[0].Tags)

	// role flipped without deregisterService running, so checkID is still set
	m.state.Master = false
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.registered, 2)
	assert.Equal(t, []string{"readonly"}, client.registered[1].Tags)

	// same role, only the TTL is updated
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.registered, 2)
	assert.Len(t, client.ttlUpdates, 3)
}

func TestManager_UpdateServiceSkipsTTLWhenReRegisterFails(t *testing.T) {
	client := &fakeClient{}
	m := newTestManager(client)

	m.state.Master = true
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.ttlUpdates, 1)

	// the check still belongs to the master-tagged entry, so passing it would keep that entry alive
	client.registerErr = errors.New("Unexpected response code: 500")
	m.state.Master = false
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.ttlUpdates, 1)

	client.registerErr = nil
	m.setConsulCheckStatus(state.Redis{})
	assert.Equal(t, []string{"readonly"}, client.registered[len(client.registered)-1].Tags)
	assert.Len(t, client.ttlUpdates, 2)
}

func TestManager_WithoutOwnService(t *testing.T) {
	m := newTestManager(&fakeClient{})
	own := serviceEntry("10.0.0.1", 6379)
	other := serviceEntry("10.0.0.2", 6379)

	assert.Equal(t, []*consulapi.ServiceEntry{other}, m.withoutOwnService([]*consulapi.ServiceEntry{own, other}))

	// without an announced host or node name the entries can't be told apart
	m.config.announceHost = ""
	assert.Equal(t, []*consulapi.ServiceEntry{own, other}, m.withoutOwnService([]*consulapi.ServiceEntry{own, other}))
}

func TestManager_WithoutOwnServiceByNodeName(t *testing.T) {
	m := newTestManager(&fakeClient{})

	// localhost redis-addr: port-only registration, Consul fills in the node IP
	m.config.announceAddr = ":6379"
	m.config.announceHost = ""
	m.config.nodeName = "node-a"

	own := portOnlyEntry("node-a", "10.0.0.1", 6379)
	other := portOnlyEntry("node-b", "10.0.0.2", 6379)
	// another resec on our node, e.g. a second Redis container
	neighbour := portOnlyEntry("node-a", "10.0.0.1", 6380)

	assert.Equal(t, []*consulapi.ServiceEntry{other, neighbour}, m.withoutOwnService([]*consulapi.ServiceEntry{own, other, neighbour}))
}

func TestManager_WatcherIgnoresOwnPortOnlyEntry(t *testing.T) {
	client := &fakeClient{
		nodeName: "node-a",
		health: func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
			return []*consulapi.ServiceEntry{
				portOnlyEntry("node-a", "10.0.0.1", 6379),
				portOnlyEntry("node-b", "10.0.0.2", 6379),
			}, &consulapi.QueryMeta{LastIndex: 5}, nil
		},
	}
	m := newTestManager(client)
	m.config.announceAddr = ":6379"
	m.config.announceHost = ""
	defer close(m.stopCh)

	go m.watchConsulMasterService()

	waitForMaster(t, m, "10.0.0.2", 6379)
}

func TestManager_WatcherIgnoresOwnStaleMasterEntry(t *testing.T) {
	client := &fakeClient{
		health: func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
			return []*consulapi.ServiceEntry{
				serviceEntry("10.0.0.1", 6379),
				serviceEntry("10.0.0.2", 6379),
			}, &consulapi.QueryMeta{LastIndex: 5}, nil
		},
	}
	m := newTestManager(client)
	defer close(m.stopCh)

	go m.watchConsulMasterService()

	waitForMaster(t, m, "10.0.0.2", 6379)
}

func TestManager_WatcherReevaluatesUnchangedCatalogWithoutMaster(t *testing.T) {
	var mu sync.Mutex
	var once sync.Once
	services := []*consulapi.ServiceEntry{}
	firstPoll := make(chan struct{})

	client := &fakeClient{
		health: func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
			mu.Lock()
			defer mu.Unlock()
			once.Do(func() { close(firstPoll) })
			return services, &consulapi.QueryMeta{LastIndex: 5}, nil
		},
	}
	m := newTestManager(client)
	defer close(m.stopCh)

	go m.watchConsulMasterService()

	// the first poll returns the empty catalog at index 5
	<-firstPoll

	// a master shows up without the index moving
	mu.Lock()
	services = []*consulapi.ServiceEntry{serviceEntry("10.0.0.2", 6379)}
	mu.Unlock()

	waitForMaster(t, m, "10.0.0.2", 6379)
}
