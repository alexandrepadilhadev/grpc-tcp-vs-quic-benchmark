# Sistema de pagamentos — plano de implementação

> **Para agentes:** SUB-SKILL OBRIGATÓRIA: use superpowers:subagent-driven-development (recomendado) ou superpowers:executing-plans para executar este plano tarefa por tarefa. Os passos usam checkbox (`- [ ]`).

**Objetivo:** substituir o `BenchService` do POC pelo gateway de pagamentos do TCC (Gateway → Core → Ledger em gRPC h2|h3, Core ⇄ RabbitMQ ⇄ Risk, Ledger → PostgreSQL), mantendo o loadgen, o netem, o runner e a análise, e preparar as rodadas da matriz.

**Arquitetura:** três saltos gRPC síncronos, todos com o mesmo `TRANSPORT` e uma conexão `transport.Client` por par; o caminho assíncrono (AMQP) e o SQL ficam numa rede sem netem. A lógica de domínio fica em `internal/<serviço>`, atrás de interfaces de função ou de `Store`, para ser testável no Windows sem broker nem banco. Os `cmd/<serviço>` só ligam as peças.

**Stack:** Go 1.27.1, connect-go v1.21.0 (`connect.WithGRPC()`), quic-go v0.63.0, protobuf v1.36.12, buf v1.73.0, `pgx/v5`, `amqp091-go`, PostgreSQL 18, RabbitMQ 4, Docker Compose, bash, Python 3.13 (pandas, pytest).

**Spec:** [`payment-plan.md`](payment-plan.md) (rev. 1, aprovada). Leia a spec antes de cada tarefa; este plano só registra as decisões que a spec não fixa.

## Restrições globais

- `internal/transport` não muda (API e comportamento).
- Um único `TRANSPORT` vale para todos os saltos gRPC; `dials = 1` em cada `transport.Client`.
- Todo cliente connect usa `connect.WithGRPC()` e `connect.WithAcceptCompression("gzip", nil, nil)`; todo handler usa `connect.WithCompression("gzip", nil, nil)`.
- Fan-out = 1: cada RPC de entrada gera exatamente uma RPC em cada salto interno. Nenhuma nova tentativa em lugar nenhum.
- O prazo vem só do loadgen (`--deadline`, padrão 2 s) e é propagado pelo `ctx` em todo salto, inclusive no `pgx`.
- Configuração: flag > env > padrão; erro de configuração sai com código ≠ 0 antes de abrir socket.
- Logs `slog` JSON em stdout. Logs de domínio e `rpclog` só em `debug`; nada novo em `info` no caminho por RPC.
- Contrato de saída sem mudança: `requests.csv` (`seq,worker,start_ns,latency_us,status,req_bytes,resp_bytes,phase`), `meta.json`, `stats.csv`, `events.csv`.
- Código, identificadores, comentários, logs e mensagens de commit em inglês e curtos.
- Testes Go só com a stdlib `testing`; Python com `pytest`.
- **Commits e push são do usuário.** Cada tarefa termina sugerindo a mensagem de commit; o executor não commita.
- Windows: só `go build ./...`, `go vet ./...`, `go test ./...` e `pytest analysis`. Docker, `tc` e bash só na VM.

## Pontos de atenção na revisão

As cinco situações que a spec implica, que nenhum teste "do caminho feliz" cobre, e que mais podem enganar; cada uma tem um teste na tarefa dona:

1. **Prazo vencido no meio do Ledger:** a transação não pode deixar débito sem lançamento nem lançamento sem débito. → Tarefa 4 (`TestMemStoreCanceledContext`) e Tarefa 9 (`TestPgStoreCanceledContext`).
2. **Débitos concorrentes na mesma conta:** o saldo final é exato. → Tarefa 4 (`TestMemStoreConcurrentSameAccount`) e Tarefa 9 (`TestPgStoreConcurrentSameAccount`).
3. **Salto interno fora do ar:** o cliente recebe `unavailable` dentro do prazo, sem travar. → Tarefa 7 (`TestCoreDown`).
4. **Broker cai durante a rodada:** o serviço sai com erro (a repetição falha de forma visível) em vez de seguir contando zero. → Tarefa 10 (`TestPublisherStopsOnError`, `TestResultsStopsWhenClosed`, `TestRiskStopsWhenClosed`).
5. **`NETEM_SUBNET` sem interface correspondente:** o contêiner não sobe sem netem em silêncio. → Tarefa 11 (verificação na VM).

---

## Fase P1 — Contratos, serviços gRPC e loadgen (Windows)

### Tarefa 1: Contrato `payment.v1` e código gerado

**Arquivos:**
- Criar: `proto/payment/v1/payment.proto`
- Criar (gerado): `gen/payment/v1/payment.pb.go`, `gen/payment/v1/paymentv1connect/payment.connect.go`

**Interfaces:**
- Produz: pacote `paymentv1` (`github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/payment/v1`) e `paymentv1connect`, com `PaymentServiceHandler/Client`, `CoreServiceHandler/Client`, `LedgerServiceHandler/Client`, as mensagens e o enum `PaymentStatus` da spec §5.

- [ ] **Passo 1 (usuário):** criar a tag do POC antes de qualquer remoção: `git tag poc-v1 59e95eb`.
- [ ] **Passo 2:** escrever `payment.proto` exatamente como na spec §5.
- [ ] **Passo 3:** gerar: `go tool -modfile=tools/go.mod buf generate`. Esperado: `gen/payment/v1/…` criado. O `clean: true` do `buf.gen.yaml` regera `gen/bench` também, que continua existindo até a Tarefa 8.
- [ ] **Passo 4:** `go build ./...` passa.
- [ ] **Passo 5:** sugerir o commit: `Add payment.v1 contract`.

### Tarefa 2: `rpclog` com lado e `payment_id`

**Arquivos:**
- Modificar: `internal/rpclog/rpclog.go`
- Modificar: `internal/rpclog/rpclog_test.go` (reescrito sem `benchsvc`)

**Interfaces:**
- Consome: `paymentv1`, `paymentv1connect` (Tarefa 1).
- Produz: `rpclog.Interceptor(log *slog.Logger) connect.Interceptor` (mesma assinatura). Cada linha `rpc` ganha `side` (`client` se `req.Spec().IsClient`, senão `server`) e `payment_id`, quando `req.Any()` implementa `interface{ GetPaymentId() string }` e o valor não é vazio.

- [ ] **Passo 1: testes que falham.** Reescrever o helper `call` com um `LedgerServiceHandler` mínimo no próprio teste (devolve `SETTLED`, ou o erro pedido) e o interceptor registrado **no servidor e no cliente**, num logger cada:

```go
func TestLogsServerAndClient(t *testing.T) // 1 linha em cada logger
// servidor: side=="server", code=="ok", payment_id=="p1",
//           procedure==paymentv1connect.LedgerServiceSettleProcedure, duration_us >= 0
// cliente:  side=="client", payment_id=="p1"
func TestLogsError(t *testing.T) // handler devolve CodeUnavailable → code=="unavailable" nas duas linhas
func TestSilentAtInfo(t *testing.T) // logger em info → 0 linhas
```

- [ ] **Passo 2:** `go test ./internal/rpclog/` → FAIL (`side` e `payment_id` ausentes).
- [ ] **Passo 3:** implementar os dois campos em `Interceptor`.
- [ ] **Passo 4:** `go test ./internal/rpclog/` → PASS.
- [ ] **Passo 5:** sugerir o commit: `Log side and payment_id per RPC`.

### Tarefa 3: Pacote `internal/service` (molde dos serviços)

**Arquivos:**
- Criar: `internal/service/flags.go`, `internal/service/server.go`, `internal/service/client.go`, `internal/service/report.go`
- Criar: `internal/service/service_test.go`
- Criar: `internal/payment/validate.go`, `internal/payment/validate_test.go`

**Interfaces:**
- Consome: `transport`, `rpclog`.
- Produz:

```go
// flags.go
type Flags struct{ /* *flag.FlagSet + env errors */ }
func NewFlags(name string) *Flags                 // FlagSet with ContinueOnError
func (f *Flags) FlagSet() *flag.FlagSet
func (f *Flags) String(p *string, name, env, def, usage string)
func (f *Flags) Duration(p *time.Duration, name, env string, def time.Duration, usage string)
func (f *Flags) Int(p *int, name, env string, def int, usage string)
func (f *Flags) Level(p *slog.Level, name, env string, def slog.Level, usage string)
// Parse parses args; an invalid env value is an error unless its flag was set.
func (f *Flags) Parse(args []string) error

type Common struct {
	Transport transport.Config // RoleServer|RoleClient; zero value when r == 0
	Level     slog.Level       // --log-level / LOG_LEVEL, default info
	RepDir    string           // --rep-dir / REP_DIR, "" = no report
}
// RegisterCommon binds the shared flags; r == 0 skips the transport flags (risk).
func RegisterCommon(f *Flags, r transport.Role) *Common

// server.go
func NewLogger(level slog.Level) *slog.Logger      // JSON to stdout
func HandlerOptions(log *slog.Logger) []connect.HandlerOption // no gzip + rpclog
// Serve mounts h at path plus GET /healthz, listens with cfg, logs "<name> started"
// (version, transport, addr, log_level) at info and blocks until ctx is done.
func Serve(ctx context.Context, name, version string, cfg transport.Config, log *slog.Logger, path string, h http.Handler) error
// Healthcheck GETs /healthz on cfg.Addr (localhost) over cfg.Transport; moved from cmd/server.
func Healthcheck(ctx context.Context, cfg transport.Config) error

// client.go
// NewClient opens one transport.Client and returns the connect options every
// service client uses: WithGRPC, no gzip, rpclog, VerifyProto.
func NewClient(cfg transport.Config, log *slog.Logger) (*transport.Client, []connect.ClientOption, error)
// VerifyProto fails a response whose x-served-proto differs from t with
// connect.CodeInternal wrapping transport.ErrProtoMismatch (checked on every response).
func VerifyProto(t transport.Transport) connect.Interceptor

// report.go
type Report struct {
	Service   string           `json:"service"`
	Version   string           `json:"version"`
	Transport string           `json:"transport"`
	Dials     map[string]int64 `json:"dials"`
	Counters  map[string]int64 `json:"counters"`
}
// WriteReport writes dir/svc-<Service>.json; dir == "" is a no-op.
func WriteReport(dir string, r Report) error

// internal/payment/validate.go
// Validate returns connect.CodeInvalidArgument unless 1 <= len(paymentID) <= 64,
// accountID > 0 and amountCents > 0.
func Validate(paymentID string, accountID uint32, amountCents int64) error
```

- [ ] **Passo 1: testes que falham** (`service_test.go`, `validate_test.go`):

```go
func TestHealthcheck(t *testing.T)     // h2 e h3: Serve num 127.0.0.1:0 → Healthcheck == nil (portado de cmd/server)
func TestHealthcheckDown(t *testing.T) // h3, 127.0.0.1:1, HandshakeTimeout 300ms → erro em < 2 s
func TestFlagPrecedence(t *testing.T)  // env CORE_ADDR=a, flag --core-addr=b → "b"; só env → "a"; nada → default
func TestInvalidEnv(t *testing.T)      // LOG_LEVEL=nope → Parse falha; com --log-level=debug → ok
func TestVerifyProto(t *testing.T)     // httptest com EnableHTTP2 + StartTLS, ledger falso atrás de um middleware que põe
                                       // x-served-proto "HTTP/1.1"; cliente srv.Client() + WithGRPC + VerifyProto(H2)
                                       // → connect.CodeOf(err) == CodeInternal && errors.Is(err, transport.ErrProtoMismatch)
func TestWriteReport(t *testing.T)     // dir temporário → svc-core.json com os campos; dir "" → nil e nenhum arquivo
func TestValidate(t *testing.T)        // ("p",1,1) ok; ("",1,1), (65×"a",1,1), ("p",0,1), ("p",1,0), ("p",1,-5) → InvalidArgument
```

- [ ] **Passo 2:** `go test ./internal/service/ ./internal/payment/` → FAIL (pacotes não existem).
- [ ] **Passo 3:** implementar. `Healthcheck` é o `healthcheck` de [cmd/server/main.go](../../cmd/server/main.go) movido sem mudança. `Flags.Parse` reproduz a regra do `cmd/server` para `LOG_LEVEL`: o erro de env só vale se a flag não foi passada.
- [ ] **Passo 4:** `go test ./internal/service/ ./internal/payment/` → PASS.
- [ ] **Passo 5:** sugerir o commit: `Add service scaffolding package`.

### Tarefa 4: Ledger (handler + `MemStore`)

**Arquivos:**
- Criar: `internal/ledger/ledger.go` (handler e `Store`), `internal/ledger/memstore.go`
- Criar: `internal/ledger/ledger_test.go`

**Interfaces:**
- Consome: `paymentv1`, `payment.Validate`.
- Produz:

```go
type Entry struct {
	PaymentID   string
	AccountID   uint32
	AmountCents int64
	AuthCode    string
}
// Store applies an entry atomically: an already-settled PaymentID returns SETTLED
// without debiting again; a missing or short account returns DECLINED and changes nothing.
type Store interface {
	Settle(ctx context.Context, e Entry) (paymentv1.PaymentStatus, error)
}

func NewMemStore(accounts int, balanceCents int64) *MemStore // ids 1..accounts
func (m *MemStore) Balance(id uint32) int64
func (m *MemStore) Entries() int

type Handler struct{ /* … */ }
func New(s Store, log *slog.Logger) *Handler // implements paymentv1connect.LedgerServiceHandler
```

- **Erros do handler:** `payment.Validate` e `auth_code` não vazio → `InvalidArgument`; erro do `Store` com `ctx.Err() != nil` → `connect.NewError(connect.CodeOf(ctx.Err()), err)`; outro erro → `Internal`.
- **Logs `debug`:** `settle received`, depois `debit applied` (com `balance_cents` só no `MemStore`; no `PgStore`, sem o campo) ou `declined`.

- [ ] **Passo 1: testes que falham:**

```go
func TestSettle(t *testing.T)                    // MemStore(10, 10_000): Settle p1/conta 3/1000 → SETTLED; Balance(3)==9000; Entries()==1
func TestSettleIdempotent(t *testing.T)          // p1 duas vezes → SETTLED nas duas; Balance(3)==9000; Entries()==1
func TestSettleDeclined(t *testing.T)            // 20_000 → DECLINED; conta 99 (inexistente) → DECLINED; saldos e Entries inalterados
func TestSettleInvalid(t *testing.T)             // amount 0 / auth_code "" → CodeInvalidArgument
func TestMemStoreCanceledContext(t *testing.T)   // ctx cancelado antes → erro, Balance e Entries inalterados
func TestMemStoreConcurrentSameAccount(t *testing.T) // 100 goroutines, conta 7, 100 ids distintos, 10 cada → Balance(7)==10_000-1000
```

- [ ] **Passo 2:** `go test ./internal/ledger/` → FAIL.
- [ ] **Passo 3:** implementar. O `MemStore` usa um `sync.Mutex` e checa `ctx.Err()` antes de mudar qualquer coisa.
- [ ] **Passo 4:** `go test -race ./internal/ledger/` → PASS. No Windows, o `-race` precisa de cgo; se não houver gcc, rodar sem `-race`. A VM roda com `-race` no `make test`.
- [ ] **Passo 5:** sugerir o commit: `Add ledger service with in-memory store`.

### Tarefa 5: Core (handler, adquirente stub, fila de eventos)

**Arquivos:**
- Criar: `internal/core/core.go`, `internal/core/events.go`
- Criar: `internal/core/core_test.go`

**Interfaces:**
- Consome: `paymentv1connect.LedgerServiceClient`, `payment.Validate`.
- Produz:

```go
// Events is the bounded in-memory queue between the RPC path and the AMQP publisher.
func NewEvents(size int) *Events
func (e *Events) Offer(ev *paymentv1.PaymentEvent) bool // never blocks; false (and Dropped+1) when full
func (e *Events) C() <-chan *paymentv1.PaymentEvent
func (e *Events) Dropped() int64

// AuthCode is the stub acquirer's code: the first 6 chars of the unpadded
// base32 (std alphabet) of FNV-1a 64 over paymentID.
func AuthCode(paymentID string) string

func New(ledger paymentv1connect.LedgerServiceClient, ev *Events, acquirerDelay time.Duration, log *slog.Logger) *Handler
// Handler implements paymentv1connect.CoreServiceHandler.
```

- **Ordem do `ProcessPayment`** (spec §6.2):
  1. `Validate`;
  2. espera `acquirerDelay`, respeitando o `ctx`;
  3. `AuthCode`;
  4. `Offer(PaymentEvent{…, CreatedNs: time.Now().UnixNano()})`;
  5. `ledger.Settle` com o mesmo `ctx`;
  6. devolve `status` e `auth_code`.
- **Erro do Ledger:** é repassado com o mesmo código (`connect.NewError(connect.CodeOf(err), err)`).
- **Logs `debug`:** `payment received`, `acquirer approved` (`auth_code`), `event queued` (ou `event dropped`).

- [ ] **Passo 1: testes que falham** (ledger falso = struct no teste que implementa `LedgerServiceClient`):

```go
func TestProcessPayment(t *testing.T)     // → SETTLED; auth_code == AuthCode("p1"), len 6; 1 evento em C() com payment_id/account/amount iguais
func TestAuthCodeDeterministic(t *testing.T) // AuthCode("p1")==AuthCode("p1"); != AuthCode("p2")
func TestEventsFull(t *testing.T)         // NewEvents(1): 2 pagamentos → os 2 SETTLED; Dropped()==1; len(C())==1
func TestLedgerErrorPropagates(t *testing.T) // ledger devolve CodeUnavailable → CodeUnavailable
func TestProcessInvalid(t *testing.T)     // payment_id "" → InvalidArgument; ledger não é chamado
```

- [ ] **Passo 2:** `go test ./internal/core/` → FAIL.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** `go test ./internal/core/` → PASS.
- [ ] **Passo 5:** sugerir o commit: `Add core service with stub acquirer and event queue`.

### Tarefa 6: Gateway

**Arquivos:**
- Criar: `internal/gateway/gateway.go`, `internal/gateway/gateway_test.go`

**Interfaces:**
- Consome: `paymentv1connect.CoreServiceClient`, `payment.Validate`.
- Produz: `func New(core paymentv1connect.CoreServiceClient, log *slog.Logger) *Handler`, que implementa `paymentv1connect.PaymentServiceHandler`. Valida, chama `ProcessPayment` com o mesmo `ctx` e devolve `{payment_id, status, auth_code}`. Erro do Core é repassado com o mesmo código. Log `debug`: `payment received`.

- [ ] **Passo 1: testes que falham:**

```go
func TestCreatePayment(t *testing.T)     // core falso → SETTLED/"ABC123" → resposta com payment_id "p1", SETTLED, "ABC123"
func TestCreateInvalid(t *testing.T)     // payment_id com 65 chars → InvalidArgument; core não é chamado
func TestCoreErrorPropagates(t *testing.T) // core → CodeDeadlineExceeded → CodeDeadlineExceeded
```

- [ ] **Passo 2:** `go test ./internal/gateway/` → FAIL.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** `go test ./internal/gateway/` → PASS.
- [ ] **Passo 5:** sugerir o commit: `Add gateway service`.

### Tarefa 7: Loadgen no `CreatePayment` + teste e2e da cadeia

**Arquivos:**
- Modificar: `cmd/loadgen/main.go`
- Modificar: `cmd/loadgen/main_test.go` (reescrito sem `benchsvc`)

**Interfaces:**
- Consome: `gateway.New`, `core.New`, `core.NewEvents`, `ledger.New`, `ledger.NewMemStore`, `service.HandlerOptions`, `service.NewClient`, `transport.Listen`.
- **Produz (flags do loadgen):**
  - saem `--payload-bytes` e `--response-bytes`;
  - entram `--accounts` (padrão 10000) e `--log-level`/`LOG_LEVEL` (padrão info);
  - o `--target` passa a ter padrão `gateway:8443`.
- **Produz (comportamento):**
  - o pedido vem de `newPayment(accounts int) *paymentv1.CreatePaymentRequest`: `payment_id` com 16 bytes aleatórios em hex, `account_id` uniforme em `[1, accounts]` (`math/rand/v2`) e `amount_cents = 1000`;
  - `lcfg.ReqBytes = proto.Size(newPayment(accounts))`;
  - a chamada devolve `proto.Size(resp.Msg)`;
  - status ≠ `SETTLED` vira `connect.NewError(connect.CodeFailedPrecondition, …)`;
  - **logs `debug`:** antes, `payment request` (`payment_id`, `account_id`, `amount_cents`); depois, `payment response` (`payment_id`, `latency_ms`, `code`, `status`, `auth_code`). O `meta.json` não muda: `ResponseBytes` fica 0.

- [ ] **Passo 1: testes que falham.** Helper `startChain(t, tr, store ledger.Store) *chain` que sobe, em `127.0.0.1:0` e com o mesmo certificado de `transporttest.NewCertFiles`, o ledger, o core (cliente do ledger) e o gateway (cliente do core), na ordem. O `chain` expõe `gatewayAddr`, `ca`, `coreCli`/`gwCli` (`*transport.Client`), `events *core.Events` e `store`.

```go
func TestSmoke(t *testing.T) // h2 e h3, MemStore(10000, 1e15): run(--requests=1000 --warmup=0 --concurrency=50 --strict)
// → nil; requests.csv com 1000 linhas ok; meta.dials==1; chain.gwCli.Dials()==1 && chain.coreCli.Dials()==1;
//   store.Entries()==1001 (1000 + probe: fan-out 1); len(events.C())==1001 && events.Dropped()==0
func TestDeadlinePropagates(t *testing.T) // h3, store que bloqueia até ctx.Done() e guarda ctx.Err():
// cliente connect direto no gateway com ctx de 300ms → CodeDeadlineExceeded em < 1 s; store viu context.DeadlineExceeded
func TestCoreDown(t *testing.T) // h2 e h3: gateway com --core-addr num 127.0.0.1:1, HandshakeTimeout 300ms;
// CreatePayment com ctx de 2s → CodeUnavailable ou CodeDeadlineExceeded, em < 2,5 s
func TestNewPayment(t *testing.T) // 1000 chamadas com accounts=10: ids com 32 chars hex, distintos; 1 <= account_id <= 10; amount 1000
```

  Os testes de flags/config já existentes em `main_test.go` são mantidos e ajustados aos nomes novos.

- [ ] **Passo 2:** `go test ./cmd/loadgen/` → FAIL.
- [ ] **Passo 3:** implementar as mudanças no `main.go`. O `BenchService` sai daqui; nada mais o importa fora da Tarefa 8.
- [ ] **Passo 4:** `go test ./cmd/loadgen/` → PASS.
- [ ] **Passo 5:** sugerir o commit: `Drive CreatePayment from the loadgen`.

### Tarefa 8: Remover o serviço do POC

**Arquivos:**
- Remover: `proto/bench/`, `gen/bench/`, `cmd/server/`, `internal/benchsvc/`

- [ ] **Passo 1:** remover os diretórios e regerar (`go tool -modfile=tools/go.mod buf generate`). Esperado: só `gen/payment/` em `gen/`.
- [ ] **Passo 2:** `go build ./... && go vet ./... && go test ./...` → tudo passa. O `Dockerfile` e o `compose.yaml` ainda citam o `server` e só serão consertados na Tarefa 11. Isso é esperado: a P1 não roda Docker, a não ser o `make test`, que só usa a imagem do Go.
- [ ] **Passo 3:** sugerir o commit: `Remove POC bench service (kept at tag poc-v1)`.

### Fim da Fase P1 — verificação na VM

```bash
git pull && git tag --list poc-v1
make test
```

Esperado: `poc-v1` listada; `make test` termina com `ok` em todos os pacotes (`cmd/loadgen`, `internal/core`, `internal/gateway`, `internal/ledger`, `internal/loadgen`, `internal/payment`, `internal/rpclog`, `internal/service`, `internal/transport`) e sem `FAIL` nem `DATA RACE`.

**Parar e esperar a confirmação do usuário.**

---

## Fase P2 — Infraestrutura e smoke (VM)

### Tarefa 9: `PgStore` (PostgreSQL) e `cmd/ledger`

**Arquivos:**
- Criar: `internal/ledger/pgstore.go`, `internal/ledger/pgstore_test.go`
- Criar: `deploy/postgres/init.sql` (schema da spec §6.2, sem mudança)
- Criar: `cmd/ledger/main.go`, `cmd/ledger/main_test.go`
- Modificar: `go.mod`/`go.sum` (`go get github.com/jackc/pgx/v5@latest`)
- Modificar: `Makefile` (alvo `test-db`)

**Interfaces:**
- Consome: `Store`, `Entry`, `Handler` (Tarefa 4); `service.*` (Tarefa 3).
- Produz: `func NewPgStore(ctx context.Context, url string, maxConns int32) (*PgStore, error)`, `(*PgStore).Settle` (o SQL da spec §6.2, em `pgx.BeginFunc`) e `(*PgStore).Close()`.
- **`cmd/ledger`:**
  - flags `--db-url`/`DATABASE_URL` (obrigatória) e `--db-max-conns`/`DB_MAX_CONNS` (8);
  - subcomando `healthcheck`;
  - no fim, `WriteReport` com `Service: "ledger"` e `Dials` vazio.

- [ ] **Passo 1: testes que falham.** No `pgstore_test.go`, os testes usam `TEST_DATABASE_URL` e chamam `t.Skip` sem ela. Cada teste usa `payment_id` com prefixo aleatório e lê saldos por `pgx` direto.

```go
func TestPgStoreSettle(t *testing.T)                 // conta 3: SETTLED; saldo −1000; 1 linha em entries
func TestPgStoreIdempotent(t *testing.T)             // mesmo id 2× → SETTLED, débito único
func TestPgStoreDeclined(t *testing.T)               // conta 99999 → DECLINED, sem linha em entries
func TestPgStoreCanceledContext(t *testing.T)        // ctx cancelado → erro; sem linha e sem débito
func TestPgStoreConcurrentSameAccount(t *testing.T)  // 100 goroutines, conta 7 → saldo exatamente −100×1000
```

  No `cmd/ledger/main_test.go`: `TestMissingDatabaseURL` (sem a flag e sem env → `run` devolve erro antes de abrir socket).

- [ ] **Passo 2:** `go test ./internal/ledger/ ./cmd/ledger/` no Windows → os `TestPgStore*` dão SKIP, e o `TestMissingDatabaseURL` dá FAIL.
- [ ] **Passo 3:** implementar o `PgStore` e o `cmd/ledger`. Alvo novo no `Makefile`:

```make
test-db:
	$(COMPOSE) up -d --wait postgres
	docker run --rm --network bench_datanet -e TEST_DATABASE_URL=postgres://postgres:bench@postgres:5432/postgres \
		-v "$(CURDIR)":/src -v bench-gomod:/go/pkg/mod -w /src golang:1.27.1-trixie \
		go test -race -run TestPgStore ./internal/ledger/; rc=$$?; $(COMPOSE) down; exit $$rc
```

  O `test-db` depende do serviço `postgres` do `compose.yaml` (Tarefa 11); a verificação dele fica no fim da fase.

- [ ] **Passo 4:** `go test ./internal/ledger/ ./cmd/ledger/` → PASS (com SKIP dos `TestPgStore*`).
- [ ] **Passo 5:** sugerir o commit: `Add PostgreSQL store and ledger command`.

### Tarefa 10: Broker, publicador/consumidor do Core, Risk e `cmd/core` + `cmd/risk`

**Arquivos:**
- Criar: `internal/broker/broker.go`
- Criar: `internal/core/async.go`, `internal/core/async_test.go`
- Criar: `internal/risk/risk.go`, `internal/risk/risk_test.go`
- Criar: `cmd/core/main.go`, `cmd/risk/main.go`
- Modificar: `go.mod`/`go.sum` (`go get github.com/rabbitmq/amqp091-go@latest`)

**Interfaces:**
- Produz:

```go
// internal/broker
const (
	EventsQueue  = "payment.events"
	ResultsQueue = "risk.results"
)
// Dial opens one connection with a publish and a consume channel and declares
// both queues (durable=false, autoDelete=false, exclusive=false).
func Dial(url string) (*Broker, error)
func (b *Broker) Publish(ctx context.Context, queue string, m proto.Message) error // transient, application/x-protobuf
func (b *Broker) Consume(queue string, prefetch int) (<-chan []byte, error)        // auto-ack; closed when the channel drops
func (b *Broker) Close() error

// internal/core/async.go
// RunPublisher drains ev until ctx is done; it returns the first publish error.
func RunPublisher(ctx context.Context, ev *Events, publish func(context.Context, *paymentv1.PaymentEvent) error, published *atomic.Int64, log *slog.Logger) error
// RunResults counts RiskResults until ctx is done; a closed channel before that is an error.
func RunResults(ctx context.Context, in <-chan []byte, results *atomic.Int64, log *slog.Logger) error

// internal/risk
func Score(paymentID string) uint32 // FNV-1a 32 mod 100
// Run consumes events, publishes {payment_id, Score}; a closed channel before ctx is done is an error.
func Run(ctx context.Context, in <-chan []byte, publish func(context.Context, *paymentv1.RiskResult) error, consumed, published *atomic.Int64, log *slog.Logger) error
```

- **Logs `debug`:**
  - core: `event published`, `risk result received` (`score`);
  - risk: `event received`, `risk scored` (`score`), `result published`.
- **`cmd/core`:**
  - flags `--ledger-addr`/`LEDGER_ADDR` (`ledger:8443`), `--amqp-url`/`AMQP_URL` (obrigatória), `--acquirer-delay`/`ACQUIRER_DELAY` (`0s`);
  - `NewEvents(10000)`;
  - `Serve`, `RunPublisher` e `RunResults` num `errgroup` simples (`sync.WaitGroup` + primeiro erro cancela o `ctx`; sem dependência nova). Erro do broker → sai com código ≠ 0;
  - no fim, `WriteReport{Service:"core", Dials:{ledgerAddr: cli.Dials()}, Counters:{events_published, events_dropped, risk_results}}`.
- **`cmd/risk`:**
  - `RegisterCommon(f, 0)`, `--amqp-url`/`AMQP_URL`, prefetch 100;
  - no fim, `WriteReport{Service:"risk", Counters:{events_consumed, results_published}}`.

- [ ] **Passo 1: testes que falham:**

```go
func TestPublisherDrains(t *testing.T)        // 3 eventos em Events → publish chamado 3×, published==3; cancel → nil
func TestPublisherStopsOnError(t *testing.T)  // publish devolve erro → RunPublisher devolve esse erro
func TestResultsCounts(t *testing.T)          // 2 RiskResult serializados → results==2; cancel → nil
func TestResultsStopsWhenClosed(t *testing.T) // canal fechado com ctx vivo → erro
func TestScore(t *testing.T)                  // Score("p1")==Score("p1") e < 100; 1000 ids aleatórios → todos < 100
func TestRiskRun(t *testing.T)                // 2 eventos → 2 publish com Score(id); consumed==2, published==2
func TestRiskStopsWhenClosed(t *testing.T)    // canal fechado com ctx vivo → erro
```

- [ ] **Passo 2:** `go test ./internal/core/ ./internal/risk/` → FAIL.
- [ ] **Passo 3:** implementar `broker`, `async.go`, `risk`, `cmd/core` e `cmd/risk`. O `broker` não tem teste unitário; é coberto pelo smoke.
- [ ] **Passo 4:** `go build ./... && go test ./...` → PASS.
- [ ] **Passo 5:** sugerir o commit: `Add AMQP path: core publisher/consumer and risk`.

### Tarefa 11: `cmd/gateway`, Docker, Compose, netem e smoke

**Arquivos:**
- Criar: `cmd/gateway/main.go`
- Modificar: `deploy/Dockerfile`, `deploy/compose.yaml`, `deploy/netem.sh`, `scripts/gen-certs.sh`, `Makefile`

**Interfaces:**
- Consome: tudo da P1 e da P2.
- **`cmd/gateway`:**
  - flag `--core-addr`/`CORE_ADDR` (`core:8443`) e subcomando `healthcheck`;
  - no fim, `WriteReport{Service:"gateway", Dials:{coreAddr: cli.Dials()}}`.

**Decisões que a spec deixa abertas:**
- **`Dockerfile`:** o target `server` vira `services`:

  ```dockerfile
  FROM runtime AS services
  COPY --from=build /out/gateway /out/core /out/ledger /out/risk /usr/local/bin/
  ENTRYPOINT ["/usr/local/bin/netem.sh", "run"]
  ```

- **`compose.yaml`:**
  - só o `gateway` tem `build` (target `services`, `image: bench-services:local`); core, ledger e risk usam `image: bench-services:local` e `command: ["/usr/local/bin/<nome>"]`;
  - healthcheck `["CMD", "/usr/local/bin/<nome>", "healthcheck"]`, com os tempos do POC;
  - cpusets e dependências da spec §7.2;
  - todo serviço Go tem `stop_grace_period: 15s`, para o `WriteReport` rodar antes do SIGKILL (o shutdown é ≤ 10 s);
  - o `loadgen` só entra na `rpcnet`, ganha `NETEM_SUBNET` e `LOG_LEVEL: ${LOG_LEVEL:-info}` e mantém `run --name bench-loadgen`;
  - nos quatro contêineres da `rpcnet`, `NETEM_SUBNET: ${NETEM_SUBNET:-10.10.0.0/24}`: a interpolação permite o teste negativo do fim da fase;
  - postgres e rabbitmq exatamente como na spec §7.2: `command` do postgres (`synchronous_commit=off`, `max_connections=50`, `shared_buffers=256MB`), `init.sql` em `/docker-entrypoint-initdb.d/`, `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS` sem *busy-wait*; healthchecks `pg_isready -U postgres` e `rabbitmq-diagnostics -q ping`.
- **Redes:**

  ```yaml
  networks:
    rpcnet:  {driver: bridge, ipam: {config: [{subnet: 10.10.0.0/24}]}}
    datanet: {driver: bridge, ipam: {config: [{subnet: 10.20.0.0/24}]}}
  ```

- **Credenciais de teste locais:**
  - postgres: `POSTGRES_PASSWORD=bench` → `DATABASE_URL=postgres://postgres:bench@postgres:5432/postgres`;
  - rabbitmq: `RABBITMQ_DEFAULT_USER=bench`, `RABBITMQ_DEFAULT_PASS=bench` → `AMQP_URL=amqp://bench:bench@rabbitmq:5672/`. O usuário `guest` só entra via loopback.
- **Imagens:** `postgres:18.<patch>-trixie` e `rabbitmq:4.<minor>.<patch>`, **sem `-management`**. O plugin de gestão coleta estatísticas e gastaria CPU no núcleo 5 dividido; isso corrige a spec §7.1 e §15. A tag completa vem da última versão estável no Docker Hub e é registrada na spec §15 neste passo.
- **`REP_DIR`:** os serviços Go recebem `REP_DIR: ${REP_DIR:-}` e montam `../results:/results`.
- **`netem.sh`:** troca a linha `IFACE=…` por:

  ```sh
  if [ -n "${NETEM_SUBNET:-}" ]; then
  	IFACE=$(ip -o -4 addr show to "$NETEM_SUBNET" | awk '{sub(/@.*/, "", $2); print $2; exit}')
  	[ -n "$IFACE" ] || { echo "netem: no interface in $NETEM_SUBNET" >&2; exit 1; }
  else
  	IFACE=${NETEM_IFACE:-eth0}
  fi
  ```

- **`gen-certs.sh`:** `subjectAltName = DNS:gateway,DNS:core,DNS:ledger,DNS:localhost,IP:127.0.0.1`, e `CN=gateway`.
- **`Makefile`:**
  - `build` continua `$(COMPOSE) --profile load build`;
  - `smoke`, com `REP=results/smoke/$(TRANSPORT)`:

  ```make
  smoke:
  	mkdir -p results/smoke/$(TRANSPORT)
  	REP_DIR=/results/smoke/$(TRANSPORT) $(COMPOSE) up -d --wait gateway risk
  	$(COMPOSE) run --rm loadgen --target=gateway:8443 --requests=1000 --warmup=0 --strict \
  		--out=/results/smoke/$(TRANSPORT)/requests.csv; rc=$$?; sleep 2; \
  	$(COMPOSE) exec -T postgres psql -U postgres -tAc 'select count(*) from entries'; \
  	REP_DIR=/results/smoke/$(TRANSPORT) $(COMPOSE) down; exit $$rc
  ```

- [ ] **Passo 1:** implementar o `cmd/gateway`. `go build ./... && go vet ./... && go test ./...` → PASS.
- [ ] **Passo 2:** fazer as mudanças em `Dockerfile`, `compose.yaml`, `netem.sh`, `gen-certs.sh` e `Makefile`.
- [ ] **Passo 3:** sugerir o commit: `Deploy payment services with compose, two networks and netem by subnet`.

### Fim da Fase P2 — verificação na VM

```bash
git pull && rm -rf certs && make certs build
make test-db
make smoke TRANSPORT=h2
cat results/smoke/h2/svc-*.json; jq '.dials' results/smoke/h2/meta.json
make smoke TRANSPORT=h3
cat results/smoke/h3/svc-*.json; jq '.dials' results/smoke/h3/meta.json
```

Esperado:
- `make test-db`: `ok … internal/ledger`, sem `SKIP` nos `TestPgStore*`.
- Cada `smoke`: o loadgen termina com `"msg":"summary"` com `"ok":1000`, `"errors":{}` e `"dials":1`; a linha do `psql` imprime `1001`; código de saída 0.
- `svc-gateway.json`: `"dials":{"core:8443":1}`. `svc-core.json`: `"dials":{"ledger-rpc:8443":1}` e `"counters":{"events_dropped":0,"events_published":1001,"risk_results":1001}`. `svc-risk.json`: `{"events_consumed":1001,"results_published":1001}`. O `jq` imprime `1`.

Checagens manuais (com a pilha no ar: `REP_DIR= docker compose -f deploy/compose.yaml up -d --wait gateway risk`):

```bash
docker compose -f deploy/compose.yaml exec core ss -tuna   # h2: ESTAB de 10.10.0.x para 10.10.0.y:8443 (o ledger pela rpcnet)
docker compose -f deploy/compose.yaml exec core ss -un   # h3: UNCONN/ESTAB em :8443
NETEM_DELAY=5 docker compose -f deploy/compose.yaml up -d --force-recreate core
docker compose -f deploy/compose.yaml exec core tc qdisc show   # netem só na interface 10.10.0.x
docker inspect --format '{{.Name}} {{.HostConfig.CpusetCpus}}' $(docker ps -q)
NETEM_SUBNET=10.99.0.0/24 docker compose -f deploy/compose.yaml run --rm --no-deps loadgen --requests=1; echo "rc=$?"
docker compose -f deploy/compose.yaml down
```

Esperado:
- `tc qdisc show`: `qdisc netem … delay 5ms` em uma interface (a da `rpcnet`) e `noqueue`/`fq_codel` na outra;
- cpusets: gateway `2`, core `3`, ledger `4`, risk/rabbitmq/postgres `5`;
- o último `run` imprime `netem: no interface in 10.99.0.0/24` e `rc=1`.

**Parar e esperar a confirmação do usuário.**

---

## Fase P3 — Runner, análise e trace

### Tarefa 12: `run.sh` para o sistema de pagamentos

**Arquivos:**
- Modificar: `scripts/run.sh`

**Mudanças** (spec §10):
- `SERVICES=(gateway core ledger risk rabbitmq postgres)`.
- `cleanup`: um `logs --no-color --no-log-prefix <s> > "$dir/<s>.log"` para cada serviço; o `--no-log-prefix` deixa as linhas como JSON puro, o que o `timeline.py` exige. Depois, `down -v` (sem o `-v`, os volumes anônimos do postgres e do rabbitmq se acumulam a cada repetição).
- `rep` (o `run` do loadgen usa `--no-deps`: com o ambiente diferente do `up`, o Compose recriaria gateway e core no meio da repetição):
  - `export REP_DIR=/results/${dir#results/}` antes do `up -d --wait gateway risk`;
  - `--target=gateway:8443`;
  - o loadgen recebe `$LOADGEN_ARGS ${LOADGEN_EXTRA:-}`;
  - outage com `exec -T "${OUTAGE_SVC:-gateway}" netem.sh outage`;
  - `sleep "${DRAIN_SECS:-2}"` antes de sair, para o caminho assíncrono esvaziar;
  - `nstat` do gateway quando `QLOG=1`.
- `matrix`: `root=results/${RUN_PREFIX:-}$run_id`.
- O `failures.log`, a nova tentativa do probe e a alternância h2/h3 não mudam.

- [ ] **Passo 1:** implementar. `bash -n scripts/run.sh` sem erro (no Git Bash do Windows).
- [ ] **Passo 2:** sugerir o commit: `Run the payment stack in run.sh`.

### Tarefa 13: `analyze.py` — `dials` por salto e `rpc_cpu`

**Arquivos:**
- Modificar: `analysis/analyze.py` (`_rep_row`)
- Modificar: `analysis/tests/test_metrics.py`

**Interfaces:**
- Produz:
  - `hop_dials(d: Path) -> list[tuple[str, str, int]]`: os `(arquivo, peer, n)` de todos os `svc-*.json` de `d` com `n != 1`.
  - Em `_rep_row`, depois do `dials` do `meta`: se `hop_dials(d)` não estiver vazio, `_warn(f"{name}: {arquivo} {peer} dials={n}, skipped")` e `return None`.
  - Coluna `rpc_cpu = gateway_cpu + core_cpu + ledger_cpu`, quando as três existem; `cpu_per_krps` usa `rpc_cpu` e, sem ela, `server_cpu`.

- [ ] **Passo 1: testes que falham.** Estender `write_rep` com `hops=None` (dict `arquivo → {peer: n}`, gravado como `{"dials": {...}}`) e `names=("server",)` (os contêineres no `stats.csv`, cada um a 10%):

```python
def test_main_skips_bad_hop_dials(tmp_path, capsys)
#  rep com hops={"svc-gateway.json": {"core:8443": 2}} → n cai 1; stderr contém "svc-gateway.json core:8443 dials=2, skipped"
def test_main_without_hop_files_uses_meta_only(tmp_path)  # write_rep sem hops → rep válida (regra do POC)
def test_rpc_cpu(tmp_path)
#  names=("gateway","core","ledger") → rpc_cpu_mean == 30; cpu_per_krps_mean == 30 / krps
```

  O `test_main` existente continua passando sem mudança (`server_cpu`, `cpu_per_krps`).

- [ ] **Passo 2:** `pytest analysis -k "hop or rpc_cpu"` → FAIL.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** `pytest analysis` → PASS (todos).
- [ ] **Passo 5:** sugerir o commit: `Validate per-hop dials and add rpc_cpu`.

### Tarefa 14: Rodada de trace

**Arquivos:**
- Criar: `analysis/timeline.py`, `analysis/tests/test_timeline.py`
- Modificar: `scripts/run.sh` (subcomando `trace`), `Makefile` (alvo `trace`)

**Interfaces:**
- `timeline.py <dir>`: lê `<dir>/*.log` (o serviço é o nome do arquivo sem `.log`) e ignora linhas que não são JSON ou que não têm `payment_id`. Ordena por `time` com `pandas.to_datetime` (precisão de ns) e grava `<dir>/timeline.txt`:

```text
payment <payment_id> (<protocol do primeiro "rpc" do pagamento, ou "?">)
  +<ms desde a 1ª linha do pagamento, 3 casas> ms  <serviço alinhado em 8>  <msg> <k=v …>
```

  Os campos `k=v` são todos menos `time`, `level`, `msg` e `payment_id`, na ordem em que aparecem. Pagamentos ordenados pelo horário da primeira linha; uma linha em branco entre eles.
- `run.sh trace <transport> [scenario]`:
  - `root=results/trace-<timestamp>`, `dir=$root/<transport>`;
  - `export LOG_LEVEL=debug LOADGEN_EXTRA="--requests=20 --rps=5 --warmup=0"`;
  - chama `rep experiments/scenarios/${scenario:-s1-baseline}.env "$dir"` e imprime `trace_dir=$dir`.
- `make trace TRANSPORT=… [SCENARIO=…]`: roda o `run.sh trace` e depois `$(COMPOSE) run --rm --entrypoint python analysis timeline.py <dir>` (o caminho dentro do contêiner é `/results/…`).

- [ ] **Passo 1: testes que falham** (`test_timeline.py`, logs sintéticos de `loadgen.log`, `gateway.log` e `risk.log`, com horários fora de ordem entre os arquivos e uma linha de texto do netem):

```python
def test_timeline_orders_and_groups(tmp_path)
#  2 pagamentos; timeline.txt começa com "payment p1 (HTTP/3.0)"; as linhas de p1 vêm em ordem de horário,
#  a primeira com "+0.000 ms  loadgen "; a de risk de p1 aparece depois da "payment response"; p2 vem depois de p1
def test_timeline_skips_non_json_and_unkeyed(tmp_path)  # "netem: eth0 clear" e {"msg":"server started"} não aparecem
def test_timeline_fields(tmp_path)  # linha com {"msg":"risk scored","score":12} → termina em "risk scored score=12"
```

- [ ] **Passo 2:** `pytest analysis/tests/test_timeline.py` → FAIL.
- [ ] **Passo 3:** implementar `timeline.py`, o subcomando `trace` e o alvo `make trace`.
- [ ] **Passo 4:** `pytest analysis` → PASS.
- [ ] **Passo 5:** sugerir o commit: `Add trace run with per-payment timeline`.

### Fim da Fase P3 — verificação na VM

```bash
git pull && make build test-py
make trace TRANSPORT=h3
grep -c '^payment ' results/trace-*/h3/timeline.txt
SCENARIOS='*' REPS=1 make run-all
make analyze RUN=<run_id impresso>
ls results/<run_id>/s5-outage/h2/rep-1/
```

Esperado:
- `make test-py`: todos os testes passam.
- O `grep -c` imprime `21`.
- No `timeline.txt`, cada pagamento tem linhas de loadgen, gateway, core, ledger e risk; o cabeçalho de cada pagamento mostra `(HTTP/3.0)` e toda linha `rpc side=client` tem `http=HTTP/3.0` (`protocol` é o protocolo RPC, `grpc`); `risk result received` aparece depois de `payment response`.
- A matriz termina sem `failures.log` (ou só com novas tentativas de probe resolvidas).
- O `ls` mostra `requests.csv`, `meta.json`, `stats.csv`, `events.csv`, `loadgen.log`, os 6 `<serviço>.log` e os 4 `svc-*.json`.
- `results/<run_id>/analysis/summary.csv` tem as colunas `rpc_cpu_mean`, `gateway_cpu_mean`, `core_cpu_mean` e `ledger_cpu_mean`.

**Parar e esperar a confirmação do usuário.**

---

## Fase P4 — Calibração

### Tarefa 15: Varredura de taxas e escolha da carga

**Arquivos:**
- Modificar: `Makefile` (alvo `calibrate`)
- Criar: `doc/PAYMENT-RUN-ANALYSIS.md` (só a seção "Calibração")
- Modificar: `experiments/scenarios/*.env` (`LOADGEN_ARGS`)

- [ ] **Passo 1:** alvo novo no `Makefile`:

```make
RATES ?= 250 500 750 1000 1500
calibrate: build
	mkdir -p results
	for r in $(RATES); do LOADGEN_EXTRA=--rps=$$r RUN_PREFIX=calib-$$r- SCENARIOS=s1-baseline REPS=3 bash scripts/run.sh; done
```

- [ ] **Passo 2 (VM):** `make calibrate` roda a varredura e, no fim, `make calibrate-report`, que analisa cada `results/calib-*` e imprime, via `analysis/calibrate.py`, a tabela taxa × transporte (`served_frac` mínimo, `late_starts` máximo — só informativo —, p50 mediano, repetições `cpu_outliers`, CPU máxima de cada contêiner, com o núcleo 5 somado), C e a taxa alta. Uma linha com `cpu_outliers > 0` indica ruído de host na própria varredura: repita aquela taxa. Para refazer só o relatório: `make calibrate-report [CALIB='calib-*']`.
- [ ] **Passo 3:** aplicar as regras da spec §9:
  - **C** é a maior taxa em que o h3 tem `served_frac ≥ 0,99` em todas as repetições (saturação; spec §9);
  - taxa alta = ⌊0,4 × C / 50⌋ × 50;
  - preencher a tabela da calibração: taxa × transporte × (`served_frac`, `late_starts`, p50, CPU de cada contêiner).
- [ ] **Passo 4 (VM):** com a taxa alta no `s1-baseline.env`, rodar `SCENARIOS=s1-baseline REPS=3 make run-all` e `SCENARIOS=s2-loss-5-low REPS=3 make run-all`, e conferir os critérios da spec §12, P4. Se o h2 falhar no `-low`, aplicar a regra da §9 (taxa e prazo) e repetir.
- [x] **Passo 5:** `LOADGEN_ARGS` atualizado: 300 RPC/s em s1, s3 e s5; 200 RPC/s nos `-low`; prazo de 2 s mantido; `s2-loss-{1,3,5}.env` removidos pela regra de corte. Acrescentado o `make fitness` (`analysis/fitness.py`, `run.sh fitness`), pré-requisito do `run-all` e do `calibrate` (spec §9).
- [ ] **Passo 6:** sugerir o commit: `Calibrate payment load rates`.

### Fim da Fase P4 — verificação na VM

A seção "Calibração" do `doc/PAYMENT-RUN-ANALYSIS.md` registra C, a taxa alta, a taxa baixa, o prazo e a decisão sobre o s2. As duas rodadas de conferência mostram:
- no s1 com h3: todo contêiner com CPU ≤ 70%, `degraded_s = 0` e nenhuma repetição com `cpu_outlier` (o `late_starts` só é reportado; spec §9);
- no `s2-loss-5-low` com h2: `served_frac ≥ 0,99` e `error_rate ≤ 0,01`.

**Parar e esperar a confirmação do usuário.**

---

## Fase P5 — Rodada completa e documentação

### Tarefa 16: Matriz final, análise e atualização dos docs

**Arquivos:**
- Modificar: `doc/PAYMENT-RUN-ANALYSIS.md`, `doc/architecture.md` §8, `doc/POC_ARCHITECTURE.md` §1.4, §3.2 e §4, `doc/plan/payment-plan.md` (status rev. 2)

- [ ] **Passo 1 (VM):** com a vCPU reservada no Hyper-V e o host parado, duas rodadas: `REPS=30 make run-all` duas vezes. Cada uma começa com o `make fitness` e não roda se o host estiver degradado.
- [ ] **Passo 2 (VM):** `make analyze RUN="<A> <B>" OUT=pooled-payment-<data>`, e cada rodada sozinha, para conferir a replicação.
- [ ] **Passo 3:** escrever o `PAYMENT-RUN-ANALYSIS.md` no formato do `RUN-ALL-ANALYSIS.md`: resumo, rodadas usadas, critérios de aceite, uma seção por cenário, validade, e a tabela POC × sistema por achado (mantém, amplia ou dilui).
- [ ] **Passo 4:** atualizar o `architecture.md` §8 (um pacote `payment.v1`, redes, cpusets, `svc-*.json`, trace) e o `POC_ARCHITECTURE.md` (resultados atuais do `RUN-ALL-ANALYSIS.md`; diagrama da §2 conforme as decisões: adquirente stub, Core → Ledger, RabbitMQ e PostgreSQL).
- [ ] **Passo 5:** sugerir o commit: `Add payment system results and update docs`.

### Fim da Fase P5 — verificação na VM

Critérios da spec §12, P5:
- sem `failures.log` além das novas tentativas de probe;
- `dials = 1` em todos os saltos (nenhum aviso `dials=` no `analyze`);
- `events_dropped = 0` fora do s5: `jq -s 'map(.counters.events_dropped // 0) | add' results/<run>/s[1-3]*/*/rep-*/svc-core.json` → `0`;
- `cv_p50 < 0,10` no s1, nos `-low` e no s5;
- p99 do h2 monotônico de 1% para 5% nos `-low`;
- `recovery_s` presente no s5;
- medianas das rodadas A e B consistentes.

---

## Fora deste plano

O Cenário 4 (migração de IP) tem spec e plano próprios, depois da P5 (spec §13).
