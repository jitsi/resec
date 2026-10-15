package consul

import (
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
	health       func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error)
}

func (c *fakeClient) LockOpts(opts *consulapi.LockOptions) (*consulapi.Lock, error) {
	return nil, nil
}

func (c *fakeClient) ServiceRegister(service *consulapi.AgentServiceRegistration) error {
	c.Lock()
	defer c.Unlock()
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
		logger:  log.WithField("testing", true),
		stateCh: make(chan state.Consul, 100),
		stopCh:  make(chan interface{}),
		state:   &state.Consul{Ready: true, Healthy: true},
	}
}

func serviceEntry(address string, port int) *consulapi.ServiceEntry {
	return &consulapi.ServiceEntry{
		Node:    &consulapi.Node{Address: address},
		Service: &consulapi.AgentService{Address: address, Port: port},
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
	assert.Equal(t, []string{"resec-redis@10.0.0.1:6379"}, client.deregistered)

	emitted := <-m.stateCh
	assert.False(t, emitted.Master)
	assert.True(t, emitted.NoMasterElected())
}

func TestManager_UpdateServiceReRegistersAfterRoleChange(t *testing.T) {
	client := &fakeClient{}
	m := newTestManager(client)

	m.state.Master = true
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.registered, 1)
	assert.Equal(t, []string{"master"}, client.registered[0].Tags)

	// demoted while the deregistration did not happen (e.g. Consul returned an error)
	m.state.Master = false
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.registered, 2)
	assert.Equal(t, []string{"readonly"}, client.registered[1].Tags)

	// same role, only the TTL is updated
	m.setConsulCheckStatus(state.Redis{})
	assert.Len(t, client.registered, 2)
	assert.Len(t, client.ttlUpdates, 3)
}

func TestManager_WithoutOwnService(t *testing.T) {
	m := newTestManager(&fakeClient{})
	own := serviceEntry("10.0.0.1", 6379)
	other := serviceEntry("10.0.0.2", 6379)

	assert.Equal(t, []*consulapi.ServiceEntry{other}, m.withoutOwnService([]*consulapi.ServiceEntry{own, other}))

	// without an announced host the entries can't be told apart
	m.config.announceHost = ""
	assert.Equal(t, []*consulapi.ServiceEntry{own, other}, m.withoutOwnService([]*consulapi.ServiceEntry{own, other}))
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
	services := []*consulapi.ServiceEntry{}

	client := &fakeClient{
		health: func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
			mu.Lock()
			defer mu.Unlock()
			return services, &consulapi.QueryMeta{LastIndex: 5}, nil
		},
	}
	m := newTestManager(client)
	defer close(m.stopCh)

	go m.watchConsulMasterService()

	// let the watcher see the empty catalog and record its index
	time.Sleep(300 * time.Millisecond)

	// a master shows up without the index moving
	mu.Lock()
	services = []*consulapi.ServiceEntry{serviceEntry("10.0.0.2", 6379)}
	mu.Unlock()

	waitForMaster(t, m, "10.0.0.2", 6379)
}
