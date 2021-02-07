package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
)

var (
	UpdateRate = 1 * time.Second

	ErrRuleDoesNotExist = errors.New("rule does not exist")
)

// Manager keeps track of all the current running quota rules
type Manager struct {
	mu                 sync.Mutex
	rules              map[uint64]*Rule
	updateRate         time.Duration
	lastUpdateDuration time.Duration
	hash               *xxhash.XXHash64
}

// NewManager returns a new quota manager
func NewManager() *Manager {
	return &Manager{
		rules:      make(map[uint64]*Rule),
		updateRate: UpdateRate,
		hash:       xxhash.New64(),
	}
}

// AddRule adds a new quota rule for a specified string key
func (m *Manager) AddRule(key string, r *Rule) {
	m.mu.Lock()
	m.hash.WriteString(key)
	m.rules[m.hash.Sum64()] = r
	m.hash.Reset()
	m.mu.Unlock()
}

// GetRule looks up the current rule for a specified string key
func (m *Manager) GetRule(key string) (*Rule, error) {
	m.mu.Lock()
	_, err := m.hash.WriteString(key)
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	r, exists := m.rules[m.hash.Sum64()]
	if !exists {
		m.mu.Unlock()
		return nil, ErrRuleDoesNotExist
	}
	m.mu.Unlock()
	return r, nil
}

// Run starts the quota manager periodically updating the tracked quotas
func (m *Manager) Run() {
	ticker := time.NewTicker(m.updateRate)
	go func() {
		for {
			select {
			case <-ticker.C:
				m.addTokens()
			}
		}
	}()
}

func (m *Manager) addTokens() {
	m.mu.Lock()
	start := time.Now()
	for _, r := range m.rules {
		r.addToken()
	}

	m.lastUpdateDuration = time.Now().Sub(start)
	m.mu.Unlock()
}

// UseToken tries to use a token for a given string key and returns false if quota has been used up
func (m *Manager) UseToken(key string) (bool, error) {
	m.mu.Lock()
	var ok bool
	var err error
	_, err = m.hash.WriteString(key)
	if err != nil {
		m.mu.Unlock()
		return ok, err
	}
	if r, exists := m.rules[m.hash.Sum64()]; exists {
		ok = r.useToken()
	} else {
		err = fmt.Errorf("key %s does not exist as a quota rule", key)
	}
	m.hash.Reset()
	m.mu.Unlock()
	return ok, err
}

// Rule represents a quota rule where queries per second and a window duration must be specified. If
// QPS is 2 and a window of 3 seconds is specified then in a 3 second window, 6 queries are allowed.
type Rule struct {
	qps        int
	window     time.Duration
	count      int
	maxQueries int
	numTokens  int
}

// NewRule creates a quota rule given a qps and time window duration
func NewRule(qps int, window time.Duration) *Rule {
	return &Rule{
		qps:        qps,
		window:     window,
		count:      0,
		maxQueries: int(window.Seconds() * float64(qps)),
		numTokens:  int(UpdateRate.Seconds() * float64(qps)),
	}
}

func (r *Rule) addToken() {
	if r.count == r.maxQueries {
		return
	}
	r.count += r.numTokens
	if r.count > r.maxQueries {
		r.count = r.maxQueries
	}
}

func (r *Rule) useToken() bool {
	if r.count == 0 {
		return false
	}
	r.count--
	return true
}
