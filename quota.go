package main

import (
	"errors"
	"sync"
	"time"

	"github.com/OneOfOne/xxhash"
)

var (
	// UpdateRate represents the time interval to update all available tokens for each rule
	UpdateRate = 1 * time.Second

	// ErrRuleDoesNotExist is returned when a rule for a key string cannot be found
	ErrRuleDoesNotExist = errors.New("rule does not exist")

	// ErrQuotaExceeded is returned when a rule has exceeded its quota
	ErrQuotaExceeded = errors.New("rule quota exceeded")
)

// Manager keeps track of all the current running quota rules
type Manager struct {
	sync.Mutex
	rules map[uint64]*Rule
	hash  *xxhash.XXHash64
}

// NewManager returns a new quota manager
func NewManager() *Manager {
	return &Manager{
		rules: make(map[uint64]*Rule),
		hash:  xxhash.New64(),
	}
}

// AddRule adds a new quota rule for a specified string key
func (m *Manager) AddRule(key string, r *Rule) {
	m.Lock()
	m.hash.WriteString(key)
	m.rules[m.hash.Sum64()] = r
	m.hash.Reset()
	m.Unlock()
}

// GetRule looks up the current rule for a specified string key
func (m *Manager) GetRule(key string) (*Rule, error) {
	m.Lock()
	m.hash.WriteString(key)
	r, exists := m.rules[m.hash.Sum64()]
	if !exists {
		m.hash.Reset()
		m.Unlock()
		return nil, ErrRuleDoesNotExist
	}
	m.hash.Reset()
	m.Unlock()
	return r, nil
}

// Run starts the quota manager periodically updating the tracked quotas
func (m *Manager) Run() {
	ticker := time.NewTicker(UpdateRate)
	go func() {
		for {
			select {
			case <-ticker.C:
				m.addTokens()
			}
		}
	}()
}

// UseToken tries to use a token for a given string key and returns nil if used
func (m *Manager) UseToken(key string) error {
	m.Lock()
	m.hash.WriteString(key)
	r, exists := m.rules[m.hash.Sum64()]
	if !exists {
		m.hash.Reset()
		m.Unlock()
		return ErrRuleDoesNotExist
	}
	used := r.useToken()
	if !used {
		m.hash.Reset()
		m.Unlock()
		return ErrQuotaExceeded
	}
	m.hash.Reset()
	m.Unlock()
	return nil
}

// addTokens runs through all rules and adds tokens to each one
func (m *Manager) addTokens() {
	m.Lock()
	for _, r := range m.rules {
		r.addToken()
	}
	m.Unlock()
}

// Rule represents a quota rule where queries per second and a window duration must be specified. If
// QPS is 2 and a window of 3 seconds is specified then in a 3 second window, 6 queries are allowed.
type Rule struct {
	qps        int
	window     time.Duration
	count      int // will always be capped to maxQueries and each use will decrement by 1
	maxQueries int
	addTokens  int
}

// NewRule creates a quota rule given a qps and time window duration
func NewRule(qps int, window time.Duration) *Rule {
	maxQueries := int(window.Seconds() * float64(qps))
	return &Rule{
		qps:        qps,
		window:     window,
		count:      maxQueries,
		maxQueries: maxQueries,
		addTokens:  int(UpdateRate.Seconds() * float64(qps)),
	}
}

// QPS returns the queries per second of the rule
func (r *Rule) QPS() int {
	return r.qps
}

// Window returns the time window of the rule
func (r *Rule) Window() time.Duration {
	return r.window
}

func (r *Rule) addToken() {
	if r.count == r.maxQueries {
		return
	}
	r.count += r.addTokens
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
