package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cedar2025/xboard-node/internal/cert"
	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/limiter"
	"github.com/cedar2025/xboard-node/internal/model"
	"github.com/cedar2025/xboard-node/internal/tracker"
	"golang.org/x/time/rate"
)

type fakeKernel struct {
	running bool

	startErr  error
	updateErr error
	addErr    error

	startCalls  int
	updateCalls int
	addCalls    int
	removeCalls int

	onUpdateUsers func([]model.UserSpec)
	onAddUsers    func([]model.UserSpec)
	onRemoveUsers func([]model.UserSpec)

	speedLimitFunc  func(string) *rate.Limiter
	deviceLimitFunc func(string) (int, bool)
}

func (f *fakeKernel) Name() string { return "fake" }
func (f *fakeKernel) Protocols() []string { return []string{"vless"} }
func (f *fakeKernel) Capabilities() kernel.Capabilities { return kernel.Capabilities{} }
func (f *fakeKernel) Start(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	_, _, _ = nodeConfig, users, tls
	f.startCalls++
	if f.startErr != nil {
		return f.startErr
	}
	f.running = true
	return nil
}
func (f *fakeKernel) Stop() { f.running = false }
func (f *fakeKernel) IsRunning() bool { return f.running }
func (f *fakeKernel) Reload(nodeConfig *model.NodeSpec, users []model.UserSpec, tls kernel.TLSCert) error {
	_, _, _ = nodeConfig, users, tls
	return nil
}
func (f *fakeKernel) AddUsers(users []model.UserSpec) (int, error) {
	f.addCalls++
	if f.onAddUsers != nil {
		f.onAddUsers(users)
	}
	if f.addErr != nil {
		return 0, f.addErr
	}
	return len(users), nil
}
func (f *fakeKernel) RemoveUsers(users []model.UserSpec) (int, error) {
	f.removeCalls++
	if f.onRemoveUsers != nil {
		f.onRemoveUsers(users)
	}
	return len(users), nil
}
func (f *fakeKernel) UpdateUsers(users []model.UserSpec) (int, int, error) {
	f.updateCalls++
	if f.onUpdateUsers != nil {
		f.onUpdateUsers(users)
	}
	if f.updateErr != nil {
		return 0, 0, f.updateErr
	}
	return len(users), 0, nil
}
func (f *fakeKernel) GetUserTraffic(ctx context.Context) (map[int][2]int64, map[int]map[string]bool, int, error) {
	_ = ctx
	return nil, nil, 0, nil
}
func (f *fakeKernel) CloseConnection(ctx context.Context, connID string) error {
	_, _ = ctx, connID
	return nil
}
func (f *fakeKernel) CloseUserConnections(ctx context.Context, uuid string) error {
	_, _ = ctx, uuid
	return nil
}
func (f *fakeKernel) SetSpeedLimitFunc(fn func(uuid string) *rate.Limiter) { f.speedLimitFunc = fn }
func (f *fakeKernel) SetDeviceLimitFunc(fn func(uuid string) (int, bool)) { f.deviceLimitFunc = fn }
func (f *fakeKernel) UpdateGlobalDevices(users map[int][]string) { _ = users }
func (f *fakeKernel) ClearGlobalDevices() {}

func newTestService(k *fakeKernel) *Service {
	sharedLimiter := limiter.New()
	s := &Service{
		kernel:       k,
		tracker:      tracker.New(),
		limiter:      sharedLimiter,
		speedTracker: limiter.NewSpeedTracker(sharedLimiter),
		cert:         cert.NewManager(config.CertConfig{}),
	}
	k.SetSpeedLimitFunc(s.speedTracker.GetLimiter)
	k.SetDeviceLimitFunc(s.limiter.GetDeviceLimitByUUID)
	return s
}

func TestApplyUserUpdatePreparesLimiterBeforeKernelUpdate(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	oldUsers := []model.UserSpec{{ID: 1, UUID: "uuid-old", SpeedLimit: 4}}
	s.updateUserState(oldUsers)

	newUsers := []model.UserSpec{{ID: 2, UUID: "uuid-new", SpeedLimit: 8}}
	k.onUpdateUsers = func(users []model.UserSpec) {
		if len(users) != 1 || users[0].UUID != "uuid-new" {
			t.Fatalf("unexpected users passed to UpdateUsers: %#v", users)
		}
		if got := k.speedLimitFunc("uuid-new"); got == nil {
			t.Fatal("expected new user's limiter to be visible before kernel UpdateUsers")
		}
	}

	s.applyUserUpdate(context.Background(), newUsers, computeUserHash(newUsers))

	if got := k.updateCalls; got != 1 {
		t.Fatalf("UpdateUsers call count = %d, want 1", got)
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].UUID != "uuid-new" {
		t.Fatalf("lastUsers = %#v, want new users", s.lastUsers)
	}
	if s.speedTracker.GetLimiter("uuid-new") == nil {
		t.Fatal("expected limiter for new user after successful update")
	}
}

func TestApplyUserUpdateRestoresStateWhenKernelAndRestartFail(t *testing.T) {
	k := &fakeKernel{
		running:   true,
		updateErr: errors.New("update failed"),
		startErr:  errors.New("restart failed"),
	}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	oldUsers := []model.UserSpec{{ID: 1, UUID: "uuid-old", SpeedLimit: 4}}
	s.updateUserState(oldUsers)
	oldHash := s.lastUserHash

	newUsers := []model.UserSpec{{ID: 2, UUID: "uuid-new", SpeedLimit: 8}}
	s.applyUserUpdate(context.Background(), newUsers, computeUserHash(newUsers))

	if got := k.startCalls; got != 1 {
		t.Fatalf("Start call count = %d, want 1", got)
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].UUID != "uuid-old" {
		t.Fatalf("lastUsers = %#v, want restored old users", s.lastUsers)
	}
	if s.lastUserHash != oldHash {
		t.Fatalf("lastUserHash = %q, want %q", s.lastUserHash, oldHash)
	}
	if s.speedTracker.GetLimiter("uuid-old") == nil {
		t.Fatal("expected old limiter to be restored after rollback")
	}
	if s.speedTracker.GetLimiter("uuid-new") != nil {
		t.Fatal("expected new limiter to be removed after rollback")
	}
}

func TestApplyUserDeltaAddPreparesLimiterBeforeKernelUpdate(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	oldUsers := []model.UserSpec{{ID: 1, UUID: "uuid-old", SpeedLimit: 4}}
	s.updateUserState(oldUsers)

	delta := []model.UserSpec{{ID: 2, UUID: "uuid-new", SpeedLimit: 8}}
	k.onAddUsers = func(users []model.UserSpec) {
		if len(users) != 1 || users[0].UUID != "uuid-new" {
			t.Fatalf("unexpected users passed to AddUsers: %#v", users)
		}
		if got := k.speedLimitFunc("uuid-new"); got == nil {
			t.Fatal("expected delta user's limiter to be visible before kernel AddUsers")
		}
	}

	s.applyUserDelta(context.Background(), "add", delta)

	if got := k.addCalls; got != 1 {
		t.Fatalf("AddUsers call count = %d, want 1", got)
	}
	if s.speedTracker.GetLimiter("uuid-new") == nil {
		t.Fatal("expected limiter for delta-added user after successful update")
	}
}

// Without a restart hook (embedded use) removal falls back to the in-place
// hot-swap instead of doing nothing.
func TestApplyUserDeltaRemoveFallsBackToHotSwapWithoutHook(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState([]model.UserSpec{
		{ID: 1, UUID: "uuid-1", SpeedLimit: 4},
		{ID: 2, UUID: "uuid-2", SpeedLimit: 4},
	})

	s.applyUserDelta(context.Background(), "remove", []model.UserSpec{{ID: 1}})

	if k.removeCalls != 1 {
		t.Fatalf("RemoveUsers calls = %d, want 1 (hot-swap fallback)", k.removeCalls)
	}
	if k.startCalls != 0 {
		t.Fatalf("kernel Start calls = %d, want 0 (kernel must not be restarted)", k.startCalls)
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].ID != 2 {
		t.Fatalf("lastUsers = %#v, want only user 2", s.lastUsers)
	}
}

// Add deltas must request a service restart as well, so the kernel is rebuilt
// from the panel's user list rather than patched in place.
func TestApplyUserDeltaAddRequestsServiceRestart(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "uuid-1"}})

	var reasons []string
	s.SetRestartHook(func(reason string) { reasons = append(reasons, reason) })

	s.applyUserDelta(context.Background(), "add", []model.UserSpec{{ID: 2, UUID: "uuid-2"}})

	if len(reasons) != 1 {
		t.Fatalf("restart hook calls = %d (%v), want 1", len(reasons), reasons)
	}
	if k.addCalls != 0 {
		t.Fatalf("AddUsers calls = %d, want 0 (restart is handled by the host)", k.addCalls)
	}
	if k.startCalls != 0 {
		t.Fatalf("kernel Start calls = %d, want 0", k.startCalls)
	}
}

// Machine mode turns sync.user.delta into a full sync.users, which goes
// through applyUserUpdate. A removal there must request a service restart too,
// otherwise machine-mode nodes never kick anyone.
func TestApplyUserUpdateRequestsServiceRestartOnRemoval(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState([]model.UserSpec{
		{ID: 1, UUID: "uuid-1"},
		{ID: 5, UUID: "uuid-5"},
	})

	var reasons []string
	s.SetRestartHook(func(reason string) { reasons = append(reasons, reason) })

	s.applyUserUpdate(context.Background(), []model.UserSpec{{ID: 1, UUID: "uuid-1"}}, "")

	if len(reasons) != 1 {
		t.Fatalf("restart hook calls = %d (%v), want 1", len(reasons), reasons)
	}
	if k.updateCalls != 0 {
		t.Fatalf("UpdateUsers calls = %d, want 0 (restart is handled by the host)", k.updateCalls)
	}
}

// A full user set that is identical to the local one must not restart.
func TestApplyUserUpdateNoChangeDoesNotRestart(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	users := []model.UserSpec{{ID: 1, UUID: "uuid-1"}}
	s.updateUserState(users)

	called := false
	s.SetRestartHook(func(string) { called = true })

	s.applyUserUpdate(context.Background(), users, "")

	if called {
		t.Fatal("restart requested for an unchanged user set")
	}
}

// The very first user set (fresh start) must not trigger a restart — there is
// nothing to disconnect yet and it would cause restart churn on boot.
func TestApplyUserUpdateFirstUsersDoesNotRestart(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}

	called := false
	s.SetRestartHook(func(string) { called = true })

	s.applyUserUpdate(context.Background(), []model.UserSpec{{ID: 1, UUID: "uuid-1"}}, "")

	if called {
		t.Fatal("restart requested while there were no local users yet")
	}
}

// An add delta that changes nothing must not request a restart.
func TestApplyUserDeltaAddNoChangeDoesNotRestart(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "uuid-1", SpeedLimit: 4}})

	called := false
	s.SetRestartHook(func(string) { called = true })

	s.applyUserDelta(context.Background(), "add", []model.UserSpec{{ID: 1, UUID: "uuid-1", SpeedLimit: 4}})

	if called {
		t.Fatal("restart requested for an add delta that changes nothing")
	}
	if k.addCalls != 0 {
		t.Fatalf("AddUsers calls = %d, want 0", k.addCalls)
	}
}

// When a restart hook is wired, removal must ask the host process to restart
// xboard-node instead of touching the kernel locally.
func TestApplyUserDeltaRemoveRequestsServiceRestart(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState([]model.UserSpec{
		{ID: 1, UUID: "uuid-1"},
		{ID: 2, UUID: "uuid-2"},
	})

	var reasons []string
	s.SetRestartHook(func(reason string) { reasons = append(reasons, reason) })

	startsBefore := k.startCalls
	s.applyUserDelta(context.Background(), "remove", []model.UserSpec{{ID: 1}})

	if len(reasons) != 1 {
		t.Fatalf("restart hook calls = %d (%v), want 1", len(reasons), reasons)
	}
	if k.startCalls != startsBefore {
		t.Fatalf("kernel Start calls = %d, want %d (restart is handled by the host)", k.startCalls, startsBefore)
	}
	if !k.running {
		t.Fatal("kernel must stay running until the host performs the restart")
	}
	if len(s.lastUsers) != 1 || s.lastUsers[0].ID != 2 {
		t.Fatalf("lastUsers = %#v, want only user 2", s.lastUsers)
	}
}

// A remove delta that matches no locally known user must not trigger a restart.
func TestApplyUserDeltaRemoveUnknownUserDoesNotRestart(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "uuid-1"}})

	called := false
	s.SetRestartHook(func(string) { called = true })

	s.applyUserDelta(context.Background(), "remove", []model.UserSpec{{ID: 99}})

	if called {
		t.Fatal("restart requested for a user that is not present locally")
	}
	if k.startCalls != 0 {
		t.Fatalf("kernel Start calls = %d, want 0", k.startCalls)
	}
}

// Removing the last user still requests a service restart (the fresh start
// will simply not bring the kernel up when the panel has no users left).
func TestApplyUserDeltaRemoveLastUserRequestsServiceRestart(t *testing.T) {
	k := &fakeKernel{running: true}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	s.updateUserState([]model.UserSpec{{ID: 1, UUID: "uuid-1"}})

	called := false
	s.SetRestartHook(func(string) { called = true })

	s.applyUserDelta(context.Background(), "remove", []model.UserSpec{{ID: 1}})

	if !called {
		t.Fatal("expected restart request when the last user is removed")
	}
	if len(s.lastUsers) != 0 {
		t.Fatalf("lastUsers = %#v, want empty", s.lastUsers)
	}
}

func TestValidateNodeRuntimeRejectsUnsupportedDNSProvider(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"http"}, &model.NodeSpec{
		Protocol: "http",
		CertConfig: &config.CertConfig{
			CertMode:    "dns",
			DNSProvider: "3123123",
			Domain:      "example.com",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty error")
	}
	if got := err.Error(); !strings.HasPrefix(got, `unsupported cert_config.dns_provider "3123123" (supported: `) {
		t.Fatalf("unexpected error: %v", got)
	}
}

func TestValidateNodeRuntimeAllowsSelfManagedTLSBeforeFilesExist(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"anytls", "hysteria"}, &model.NodeSpec{
		Protocol: "anytls",
		CertConfig: &config.CertConfig{
			CertMode: "self",
			Domain:   "example.com",
		},
	}, kernel.TLSCert{})
	if err != nil {
		t.Fatalf("expected self-managed TLS config to pass validation, got %v", err)
	}
}

func TestValidateNodeRuntimeAllowsSingboxRealityWithRequiredFields(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
		TLSSettings: map[string]any{
			"private_key": "test-key",
			"server_name": "example.com",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err != nil {
		t.Fatalf("expected sing-box reality validation to pass, got %v", err)
	}
}

func TestValidateNodeRuntimeRejectsRealityWithoutTLSSettings(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "reality tls requires tls_settings" {
		t.Fatalf("unexpected error: %v", got)
	}
}

func TestValidateNodeRuntimeRejectsRealityWithoutPrivateKey(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
		TLSSettings: map[string]any{
			"server_name": "example.com",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "reality tls requires tls_settings.private_key" {
		t.Fatalf("unexpected error: %v", got)
	}
}

func TestValidateNodeRuntimeRejectsRealityWithoutServerNameOrDest(t *testing.T) {
	cfg := &config.Config{Kernel: config.KernelConfig{Type: "singbox"}}
	err := validateNodeRuntime(cfg, []string{"vless"}, &model.NodeSpec{
		Protocol: "vless",
		TLS:      2,
		TLSSettings: map[string]any{
			"private_key": "test-key",
		},
	}, kernel.TLSCert{CertPEM: []byte("CERT"), KeyPEM: []byte("KEY")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "reality tls requires tls_settings.server_name or tls_settings.dest" {
		t.Fatalf("unexpected error: %v", got)
	}
}
