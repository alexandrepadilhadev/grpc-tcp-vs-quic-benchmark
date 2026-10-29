# POC gRPC h2 vs h3 — Plano de implementação

> **Para agentes:** use `superpowers:executing-plans` (ou `superpowers:subagent-driven-development`) tarefa a tarefa. Passos com checkbox (`- [ ]`).

**Objetivo:** implementar o POC do `poc-plan.md` rev. 2 em duas entregas. Fase 1 = etapas 1–4 (código + smoke em contêiner). Fase 2 = etapas 5–7 (netem, runner, análise, rodada completa).

**Arquitetura:** o `internal/transport` isola h2/h3 atrás de `Listen`/`Serve` e `NewClient`. O `benchsvc` e o `loadgen` só dependem dele e do código gerado. Docker, netem e runner envolvem os binários sem mudar contratos.

**Stack:** Go 1.27.1, connect-go v1.21.0, quic-go v0.63.0, protobuf v1.36.12, buf v1.73.0; Python 3.13 + pandas/numpy/matplotlib/scipy/pytest; Docker Compose, Makefile, bash.

**Spec:** `doc/plan/poc-plan.md` (rev. 2). Ele manda em caso de dúvida.

**Formato enxuto (pedido do usuário):** cada tarefa traz arquivos, interfaces exatas, casos de teste (entrada → esperado) e trechos só onde a API não é óbvia. O código completo vem na implementação, sempre por TDD: teste falhando → código mínimo → teste passando.

## Restrições globais

- Go 1.27.1 (`go 1.27.0` + `toolchain go1.27.1`); versões da §11 do spec, sem outras dependências.
- HTTP/2 só via `net/http` nativo: não importar `golang.org/x/net/http2`.
- TLS 1.3 mínimo nas duas pilhas, mesmo certificado; parâmetros simétricos da tabela da §5.2.
- Testes Go só com a stdlib `testing`.
- Código, identificadores, comentários, logs e commits em inglês, curtos.
- Schema do CSV, exato: `seq,worker,start_ns,latency_us,status,req_bytes,resp_bytes,phase`.
- Servidor sem log por requisição **por padrão**. O log por RPC (`internal/rpclog`) sai em `debug` e só liga com `LOG_LEVEL=debug` (ver `closeout-plan.md`).
- No Windows só rodam `go build ./...`, `go vet ./...`, `go test ./...` e `pytest`. Docker, `tc` e `*.sh` só na VM (Ubuntu 26.04, 6 vCPUs, 8 GB).
- Scripts sempre invocados via `bash`/`sh` e copiados com `COPY --chmod=755`: o git no Windows não grava o bit `+x`.
- Commits: o usuário faz. Cada tarefa termina com a mensagem sugerida. Nunca `git push`.

## Foco de revisão

1. **Rajada antes de a conexão existir:** o h2 pode abrir conexões extras se várias RPCs saírem juntas antes da primeira conexão. Esperado: o loadgen sempre faz a RPC de teste antes de soltar os workers, e o run termina com `dials = 1`. Teste na Tarefa 9 (smoke local confere `dials == 1` no `meta.json`).
2. **Protocolo errado do outro lado:** cliente h2 contra servidor h3, h3 contra h2, ou servidor TLS sem ALPN h2. Esperado: erro rápido e claro, nunca fallback. Testes nas Tarefas 4 e 9.
3. **Env inválida** (`TRANSPORT=h4`, `IDLE_TIMEOUT=abc`). Esperado: falha na inicialização citando a variável. Teste na Tarefa 2.
4. **Falha de escrita do CSV** (disco cheio, caminho inválido). Esperado: erro e saída ≠ 0, sem deadlock dos workers. Testes nas Tarefas 5 e 9.
5. **SIGTERM/Ctrl-C no meio do run.** Esperado: o servidor encerra de forma graceful; o loadgen para rápido e ainda grava o CSV e o `meta.json`. Testes nas Tarefas 4 e 7.

---

# Fase 1 — etapas 1 a 4

### Tarefa 1: scaffold, contrato e geração de código

**Arquivos:**
- Criar: `.gitattributes`, `.gitignore`, `go.mod`, `tools/go.mod`, `buf.yaml`, `buf.gen.yaml`, `proto/bench/v1/bench.proto`
- Gerado (versionado): `gen/bench/v1/bench.pb.go`, `gen/bench/v1/benchv1connect/bench.connect.go`

**Produz:** os pacotes `github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen/bench/v1` (`benchv1`) e `.../gen/bench/v1/benchv1connect`.

- [ ] **1. Arquivos base**

`.gitattributes`:
```
* text=auto
*.sh       text eol=lf
Makefile   text eol=lf
Dockerfile text eol=lf
*.env      text eol=lf
gen/**     linguist-generated=true
```
`.gitignore`: `results/`, `certs/`, `.venv/`, `__pycache__/`, `.pytest_cache/`.

`go.mod`: `module github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark`, `go 1.27.0`, `toolchain go1.27.1`.

`tools/go.mod`: `module github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/tools`, `go 1.27.0`. Em seguida:
```bash
go get -tool -modfile=tools/go.mod github.com/bufbuild/buf/cmd/buf@v1.73.0 google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12 connectrpc.com/connect/cmd/protoc-gen-connect-go@v1.21.0
```

- [ ] **2. Configuração do buf e contrato**

`buf.yaml`:
```yaml
version: v2
modules:
  - path: proto
```
`buf.gen.yaml`:
```yaml
version: v2
clean: true
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/alexandrepadilhadev/grpc-tcp-vs-quic-benchmark/gen
plugins:
  - local: [go, tool, -modfile=tools/go.mod, protoc-gen-go]
    out: gen
    opt: paths=source_relative
  - local: [go, tool, -modfile=tools/go.mod, protoc-gen-connect-go]
    out: gen
    opt: paths=source_relative
```
`proto/bench/v1/bench.proto`: exatamente o da §5.1 do spec.

- [ ] **3. Gerar e resolver dependências**

```bash
go tool -modfile=tools/go.mod buf generate
go get connectrpc.com/connect@v1.21.0 google.golang.org/protobuf@v1.36.12
go mod tidy
go build ./...
```
Esperado: os dois arquivos em `gen/`, build sem erro. Rodar `buf generate` de novo não pode deixar diff em `gen/` (`git status --short gen/` vazio depois de `git add`).

- [ ] **4. Commit (você):** `Add proto contract and code generation`

---

### Tarefa 2: `transport` — tipo `Transport` e `Config`

**Arquivos:** criar `internal/transport/transport.go`, `internal/transport/config.go`, `internal/transport/transport_test.go`, `internal/transport/config_test.go`.

**Produz:** `Transport`, `H2`, `H3`, `Parse`, `Network`, `ALPN`, `Proto`, `Set`, `String`; `ErrInvalidTransport`, `ErrInvalidConfig`, `ErrTLSConfig`, `ErrProtoMismatch`; `Role`, `RoleServer`, `RoleClient`; `Config`, `DefaultConfig`, `RegisterFlags(fs, r)`, `Validate(r)`. As assinaturas são as da §5.2 do spec.

- [ ] **1. Testes que falham**

| Teste | Entrada | Esperado |
|---|---|---|
| `TestParse` | `"h2"`, `"h3"` | `H2`, `H3`, sem erro |
| | `""`, `"H2"`, `"h1"`, `"quic"` | `errors.Is(err, ErrInvalidTransport)`; a mensagem cita `h2, h3` |
| `TestTransportProps` | `H2` / `H3` | `Network` `tcp`/`udp`; `ALPN` `h2`/`h3`; `Proto` `HTTP/2.0`/`HTTP/3.0` |
| `TestFlagValue` | `fs.Parse(["--transport=h4"])` | erro contendo `invalid transport` |
| `TestDefaults` | sem env e sem flags | `h2`, `:8443`, 1000 streams, 5s, 30s, 10s |
| `TestPrecedence` | `t.Setenv("TRANSPORT","h3")` | `h3`; com `--transport=h2` também → `h2` (flag > env > default) |
| | `IDLE_TIMEOUT=45s`, `MAX_CONCURRENT_STREAMS=10` | campos lidos |
| `TestRoleFlags` | `RegisterFlags(fs, RoleClient)` | `fs.Lookup("tls-ca")` existe; `addr`, `tls-cert` e `max-streams` não existem |
| `TestInvalidEnv` | `TRANSPORT=h4` ou `IDLE_TIMEOUT=abc` | `Validate` retorna erro citando `TRANSPORT` / `IDLE_TIMEOUT`, com `errors.Is(err, ErrInvalidConfig)` |
| `TestValidateServer` | falta `CertFile`/`KeyFile`; `Addr=""` ou `"x"`; streams 0; `ShutdownTimeout=0` | um erro por campo, todos juntos (`errors.Join`) e todos `ErrInvalidConfig` |
| `TestValidateClient` | falta `CAFile` | `ErrInvalidConfig`; o mesmo `Config` sem cert/key passa em `RoleClient` |
| `TestValidateNoRole` | `Validate(0)` | `ErrInvalidConfig` |

Rode `go test ./internal/transport/`. Esperado: FAIL (símbolos indefinidos).

- [ ] **2. Implementação mínima**

- Variáveis de ambiente: `TRANSPORT`, `HANDSHAKE_TIMEOUT`, `IDLE_TIMEOUT`, `ADDR`, `TLS_CERT`, `TLS_KEY`, `MAX_CONCURRENT_STREAMS`, `SHUTDOWN_TIMEOUT`, `TLS_CA`.
- `RegisterFlags` sobrescreve os campos com o env e guarda os erros de parse num campo não exportado `envErr`. Depois define as flags usando os valores atuais como default. `max-streams` usa `fs.Func`, porque não existe `Uint32Var`.
- `Validate` devolve `envErr` e os erros de campo com `errors.Join`. Cada erro é `fmt.Errorf("%w: ...", ErrInvalidConfig)`, citando a flag e a env.
- `Addr` é validado com `net.SplitHostPort`; a porta 0 é aceita, para os testes.

- [ ] **3. Rode** `go test ./internal/transport/`. Esperado: PASS.
- [ ] **4. Commit (você):** `Add transport type and config`

---

### Tarefa 3: `transport` — TLS e `transporttest`

**Arquivos:** criar `internal/transport/tls.go`, `internal/transport/tls_test.go`, `internal/transport/transporttest/certs.go`.

**Produz:**
- `func serverTLSConfig(t Transport, certFile, keyFile string) (*tls.Config, error)` (não exportada)
- `func clientTLSConfig(t Transport, caFile string) (*tls.Config, error)` (não exportada)
- `func transporttest.NewCertFiles(tb testing.TB) (certFile, keyFile, caFile string)`: CA e folha ECDSA P-256 com SAN `localhost`, `server`, `127.0.0.1`, `::1`, validade de 24 h, gravadas em PEM em `tb.TempDir()`.

- [ ] **1. Testes que falham**

| Teste | Esperado |
|---|---|
| `TestServerTLS` (h2, h3) | `MinVersion == tls.VersionTLS13`, `NextProtos == [t.ALPN()]`, 1 certificado |
| `TestClientTLS` (h2, h3) | `MinVersion` TLS 1.3, `RootCAs != nil`, `NextProtos == [t.ALPN()]` |
| `TestTLSErrors` | cert ausente; key de outro par; CA com lixo; CA ausente → `errors.Is(err, ErrTLSConfig)` |
| `TestRejectsTLS12` | handshake `tls.Server`(config do servidor) × `tls.Client`(`MaxVersion: TLS12`) sobre `net.Pipe` → erro |

- [ ] **2. Implementação:** `tls.LoadX509KeyPair` para o servidor; `x509.NewCertPool` + `AppendCertsFromPEM` para o cliente (retorno `false` é erro). Os erros são `fmt.Errorf("%w: ...: %w", ErrTLSConfig, err)`.
- [ ] **3. Rode** `go test ./internal/transport/...`. Esperado: PASS.
- [ ] **4. Commit (você):** `Add TLS 1.3 config and test certs`

---

### Tarefa 4: `transport` — `Server` e `Client` (h2/h3)

**Arquivos:** criar `internal/transport/server.go`, `internal/transport/client.go`, `internal/transport/e2e_test.go` (pacote `transport_test`).

**Consome:** `Config.Validate`, `serverTLSConfig` e `clientTLSConfig` (Tarefas 2 e 3), `transporttest.NewCertFiles`.

**Produz:** `HeaderServedProto`, `Listen`, `(*Server).Addr`, `(*Server).Serve`, `NewClient`, `(*Client).Do`, `(*Client).Dials`, `(*Client).Close`, `VerifyServedProto`.

- [ ] **1. Testes que falham**

O helper `startServer(t, tr, handler, mutate func(*Config)) (addr string, cfg Config, stop func() error)` usa `Addr: "127.0.0.1:0"` e `slog.New(slog.DiscardHandler)`. As URLs são `https://127.0.0.1:<port>`.

| Teste (h2 e h3) | Esperado |
|---|---|
| `TestRoundTrip` | status 200; `resp.Proto == tr.Proto()`; header `x-served-proto == tr.Proto()`; `VerifyServedProto` nil; `Dials() == 1` |
| `TestSingleConnection` | `MaxConcurrentStreams=10`, handler dorme 20 ms, 1 requisição de aquecimento e depois 100 concorrentes → todas 200 e `Dials() == 1` |
| `TestGracefulShutdown` | handler espera 300 ms; cancela o `ctx` com a requisição em voo → resposta 200; depois de `cli.Close()`, `Serve` retorna `nil` em < 3 s |
| `TestListenErrors` | `Config` inválido → `ErrInvalidConfig`; cert ausente → `ErrTLSConfig`; segundo `Listen` no mesmo addr → erro |
| `TestNewClientErrors` | inválido → `ErrInvalidConfig`; CA ausente → `ErrTLSConfig` |

| Teste de divergência | Esperado |
|---|---|
| cliente h2 → servidor h3 | erro em < 2 s |
| cliente h3 → servidor h2 (`HandshakeTimeout=500ms`) | erro em < 2 s |
| cliente h2 → `tls.Listen` sem ALPN + `http.Serve` | `errors.Is(err, ErrProtoMismatch)` |
| `TestVerifyServedProto` | header igual → nil; ausente ou diferente → `ErrProtoMismatch` |

- [ ] **2. Implementação — servidor**

O `Server` guarda `serve func() error` e `shutdown func(context.Context) error` da pilha escolhida. `Listen` faz, em ordem: `Validate(RoleServer)` → TLS → abre o socket → monta o servidor. O handler é embrulhado por `servedProto`, que faz `w.Header().Set(HeaderServedProto, r.Proto)` antes de chamar o `next`.

```go
// h2
ln, err := net.Listen("tcp", cfg.Addr)
var p http.Protocols
p.SetHTTP2(true)
srv := &http.Server{
	Handler:           h,
	TLSConfig:         tlsCfg,
	Protocols:         &p,
	HTTP2:             &http.HTTP2Config{MaxConcurrentStreams: int(cfg.MaxConcurrentStreams)},
	ReadHeaderTimeout: cfg.HandshakeTimeout, // also bounds the TLS handshake
	IdleTimeout:       cfg.IdleTimeout,
	ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
}
// serve: srv.ServeTLS(ln, "", "")   shutdown: srv.Shutdown

// h3
udpAddr, err := net.ResolveUDPAddr("udp", cfg.Addr)
pc, err := net.ListenUDP("udp", udpAddr) // *net.UDPConn keeps GSO/ECN
srv := &http3.Server{
	Handler:     h,
	TLSConfig:   tlsCfg,
	IdleTimeout: cfg.IdleTimeout,
	QUICConfig: &quic.Config{
		MaxIncomingStreams:   int64(cfg.MaxConcurrentStreams),
		MaxIdleTimeout:       cfg.IdleTimeout,
		HandshakeIdleTimeout: cfg.HandshakeTimeout,
	},
	Logger: log,
}
// serve: srv.Serve(pc)   shutdown: srv.Shutdown
```
`Serve(ctx)` roda o `serve` numa goroutine. Quando o `ctx` termina, chama `shutdown` com `context.WithTimeout(context.Background(), cfg.ShutdownTimeout)`, espera o `serve` retornar e trata `http.ErrServerClosed` como `nil`. Se o `serve` retornar antes do `ctx`, esse erro é o retorno de `Serve`.

- [ ] **3. Implementação — cliente**

```go
// h2
var p http.Protocols
p.SetHTTP2(true)
d := &tls.Dialer{Config: tlsCfg}
tr := &http.Transport{
	Protocols:          &p,
	HTTP2:              &http.HTTP2Config{StrictMaxConcurrentRequests: true},
	IdleConnTimeout:    cfg.IdleTimeout,
	DisableCompression: true,
	DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(ctx, cfg.HandshakeTimeout)
		defer cancel()
		conn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		c.dials.Add(1)
		if got := conn.(*tls.Conn).ConnectionState().NegotiatedProtocol; got != "h2" {
			conn.Close()
			return nil, fmt.Errorf("%w: negotiated %q, want h2", ErrProtoMismatch, got)
		}
		return conn, nil
	},
}
// close: tr.CloseIdleConnections

// h3: QUIC requires ALPN, so a mismatch cannot pass the handshake; only count dials.
tr := &http3.Transport{
	TLSClientConfig:    tlsCfg,
	QUICConfig:         &quic.Config{HandshakeIdleTimeout: cfg.HandshakeTimeout, MaxIdleTimeout: cfg.IdleTimeout},
	DisableCompression: true,
	Dial: func(ctx context.Context, addr string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
		conn, err := quic.DialAddrEarly(ctx, addr, tc, qc)
		if err == nil {
			c.dials.Add(1)
		}
		return conn, err
	},
}
// close: tr.Close
```
`Do` delega a `(&http.Client{Transport: tr}).Do`.

- [ ] **4. Rode** `go test ./internal/transport/...` e `go vet ./...`. Esperado: PASS e vet limpo.
- [ ] **5. Commit (você):** `Add h2/h3 server and client`

---

### Tarefa 5: `loadgen` — `Recorder`

**Arquivos:** criar `internal/loadgen/recorder.go` e `internal/loadgen/recorder_test.go`.

**Produz:**
```go
type Phase string
const (
	PhaseWarmup  Phase = "warmup"
	PhaseMeasure Phase = "measure"
)
const Header = "seq,worker,start_ns,latency_us,status,req_bytes,resp_bytes,phase"
type Record struct {
	Seq       uint64
	Worker    int
	StartNs   int64
	LatencyUs int64
	Status    string
	ReqBytes  int
	RespBytes int
	Phase     Phase
}
type Recorder struct{ /* ch chan Record; done chan error; stalls atomic.Int64 */ }
func NewRecorder(w io.Writer, buffer int) *Recorder
func (r *Recorder) Record(rec Record) // blocks when full; never drops
func (r *Recorder) Close() error      // drains, flushes, returns first write error
func (r *Recorder) Stalls() int64
```

- [ ] **1. Testes que falham**

| Teste | Esperado |
|---|---|
| `TestRecorderFormat` | 2 registros → saída exata: `Header\n1,0,100,250,ok,1024,1024,measure\n2,1,200,300,unavailable,1024,0,warmup\n` |
| `TestRecorderConcurrent` | 50 goroutines × 1000 registros, `buffer=16` → 50.000 linhas + header, 8 campos cada, `seq` sem repetição |
| `TestRecorderWriteError` | writer que sempre falha, 10k registros → `Record` nunca trava e `Close` retorna o erro |

- [ ] **2. Implementação:** uma goroutine escritora com `bufio.Writer` de 64 KiB. Cada linha é montada com `strconv.Append*` num buffer reutilizado. Depois de um erro, a goroutine continua drenando o canal sem escrever. `Record` tenta um `select` com `default`; se o canal estiver cheio, conta um stall e faz o envio bloqueante. Chamar `Record` depois de `Close` é erro do chamador (documentar).
- [ ] **3. Rode** `go test ./internal/loadgen/`. Esperado: PASS.
- [ ] **4. Commit (você):** `Add async CSV recorder`

---

### Tarefa 6: `benchsvc`

**Arquivos:** criar `internal/benchsvc/service.go` e `internal/benchsvc/service_test.go`.

**Produz:**
```go
const MaxResponseBytes = 1 << 20
type Service struct{ /* payload []byte (random, MaxResponseBytes) */ }
func New() *Service
func (s *Service) Unary(ctx context.Context, req *connect.Request[benchv1.UnaryRequest]) (*connect.Response[benchv1.UnaryResponse], error)
```

- [ ] **1. Testes que falham** (chamada direta, sem rede)

| Entrada | Esperado |
|---|---|
| `ResponseSize=100` | payload de 100 bytes |
| `ResponseSize=0` | payload vazio |
| `ServerDelayUs=20000` | tempo decorrido ≥ 20 ms |
| `ResponseSize=MaxResponseBytes+1` | `connect.CodeOf(err) == CodeInvalidArgument` |
| `ServerDelayUs=1e6` com ctx de 10 ms | retorna em < 100 ms com `CodeDeadlineExceeded` |

- [ ] **2. Implementação:** payload aleatório (`crypto/rand`) gerado uma vez; a resposta usa `payload[:n]`. O atraso usa `time.NewTimer` + `select` com `ctx.Done()`. Erro de contexto vira `CodeDeadlineExceeded` ou `CodeCanceled`.
- [ ] **3. Rode** `go test ./internal/benchsvc/`. Esperado: PASS.
- [ ] **4. Commit (você):** `Add bench service`

---

### Tarefa 7: `loadgen` — `Runner`

**Arquivos:** criar `internal/loadgen/runner.go` e `internal/loadgen/runner_test.go`.

**Consome:** `Recorder`, `Record` e `Phase` (Tarefa 5).

**Produz:**
```go
type Config struct {
	Concurrency int           `json:"concurrency"`
	RPS         int           `json:"rps"`         // 0 = closed loop
	Duration    time.Duration `json:"duration_ns"` // ignored when Requests > 0
	Warmup      time.Duration `json:"warmup_ns"`
	Requests    int           `json:"requests"`    // measured RPCs; 0 = use Duration
	Deadline    time.Duration `json:"deadline_ns"`
	ReqBytes    int           `json:"req_bytes"`
}
func (c Config) Validate() error
// Call performs one RPC and returns the response payload size.
type Call func(ctx context.Context) (respBytes int, err error)
type Result struct {
	Start, End time.Time
	Total, OK  int64            // measure phase only
	Errors     map[string]int64 // status -> count, measure phase only
	LateStarts int64            // RPS mode: sent > 1ms after schedule
	Unsent     int64            // RPS mode: slots due before end but never sent
}
func Run(ctx context.Context, cfg Config, call Call, rec *Recorder) Result
```

- [ ] **1. Testes que falham** (`Call` falsa, sem rede)

| Teste | Esperado |
|---|---|
| `TestRequestsExact` | `Concurrency=4`, `Requests=100`, `Warmup=0` → 100 linhas `measure`, `seq` único, `worker ∈ [0,4)`, `OK == Total == 100` |
| `TestWarmupPhases` | `Warmup=50ms`, `Duration=100ms`, call de 2 ms → as duas fases aparecem; toda linha `warmup` começa antes da primeira `measure`; o `Result` só conta `measure` |
| `TestStatusMapping` | `connect.NewError(CodeUnavailable)` → `unavailable`; call bloqueada em `ctx` com `Deadline=20ms` → `deadline_exceeded` e latência ≥ 20 ms; `Errors` contados |
| `TestRPSSchedule` | `RPS=200`, `Duration=300ms`, `Concurrency=4`, call instantânea → linhas + `Unsent` == 60; todo `start_ns - start_ns[0]` é múltiplo exato de 5 ms |
| `TestCOCorrection` | `RPS=100`, `Concurrency=1`, `Requests=10`, a primeira call dura 60 ms → a linha `seq=1` tem `latency_us ≥ 50000`; `LateStarts ≥ 4` |
| `TestCancel` | `ctx` cancelado em 50 ms com `Duration=10s` → `Run` retorna em < 300 ms e as linhas já registradas ficam |
| `TestConfigValidate` | `Concurrency ≤ 0`, `RPS < 0`, `Warmup < 0`, `Deadline ≤ 0`, `Duration ≤ 0` com `Requests == 0`, `Requests < 0` → erro |

- [ ] **2. Implementação**

- `t0 := time.Now()`; `startNs(t) = t0.UnixNano() + int64(t.Sub(t0))`, que usa o relógio monotônico.
- Fase: `warmup` se o início (real ou planejado) for anterior a `t0 + Warmup`.
- Loop fechado: N workers repetem o envio até `t0 + Warmup + Duration`, ou até esgotar `Requests`. O contador atômico de `measure` só é reservado antes do envio.
- `--rps`: um pacer calcula `t_k = t0 + k·1s/RPS`, dorme até `t_k` e envia `t_k` num canal sem buffer. Se os workers estão ocupados, o pacer bloqueia, e os `t_k` atrasados saem em sequência depois. O worker mede a latência desde `t_k` e grava `start_ns = startNs(t_k)`. O pacer para no fim da janela (relógio) e conta `Unsent`.
- Cada RPC usa `context.WithTimeout(ctx, Deadline)`.
- Status:
  - `nil` → `ok`;
  - `context.DeadlineExceeded` → `deadline_exceeded`;
  - `context.Canceled` → `canceled`;
  - demais erros → `connect.CodeOf(err).String()`.

- [ ] **3. Rode** `go test ./internal/loadgen/`. Esperado: PASS.
- [ ] **4. Commit (você):** `Add closed-loop and fixed-rate runner`

---

### Tarefa 8: `cmd/server` e health check

**Arquivos:** criar `cmd/server/main.go` e `cmd/server/main_test.go`.

**Consome:** `transport.Listen`, `NewClient`, `VerifyServedProto`, `RegisterFlags(fs, RoleServer|RoleClient)`; `benchsvc.New`; `benchv1connect.NewBenchServiceHandler`.

**Produz:**
```go
var version = "dev" // -ldflags "-X main.version=..."
func run(ctx context.Context, args []string) error // "healthcheck" as args[0] runs the probe
func newMux() http.Handler                          // bench handler + GET /healthz -> 200
func healthcheck(ctx context.Context, cfg transport.Config) error
```

- [ ] **1. Testes que falham**

| Teste (h2 e h3) | Esperado |
|---|---|
| `TestHealthcheck` | `transport.Listen(cfg, newMux())` em `127.0.0.1:0`, com `cfg.Addr` = endereço real → `healthcheck` retorna nil |
| `TestHealthcheckDown` | nada escutando, `HandshakeTimeout=300ms` → erro em < 2 s |

- [ ] **2. Implementação**

- `main` usa `signal.NotifyContext(SIGINT, SIGTERM)` e chama `run`; em caso de erro, imprime no stderr e sai com 1.
- `newMux` registra o handler com `connect.WithCompression("gzip", nil, nil)`, o que desliga o gzip.
- Log JSON (`slog.NewJSONHandler(os.Stdout)`): `server started` com `version`, `transport`, `addr`, `max_streams`, `handshake_timeout`, `idle_timeout`; `server stopped` no fim.
- `healthcheck` monta a URL `https://<host>:<port>/healthz`, com o host e a porta de `cfg.Addr`. Se o host estiver vazio ou for `0.0.0.0`/`::`, usa `localhost`. A requisição tem timeout `HandshakeTimeout`; espera status 200 e `VerifyServedProto` ok.

- [ ] **3. Rode** `go test ./cmd/server/` e `go build ./...`. Esperado: PASS.
- [ ] **4. Commit (você):** `Add bench server with healthcheck`

---

### Tarefa 9: `cmd/loadgen`, `meta.json` e smoke local (etapa 3)

**Arquivos:** criar `internal/loadgen/meta.go`, `internal/loadgen/meta_test.go`, `cmd/loadgen/main.go`, `cmd/loadgen/main_test.go`.

**Consome:** `loadgen.Run`, `Config`, `NewRecorder`; `transport.NewClient`, `VerifyServedProto`, `RegisterFlags(fs, RoleClient)`; `benchsvc.MaxResponseBytes`.

**Produz:**
```go
type Meta struct {
	Version        string            `json:"version"`
	Transport      string            `json:"transport"`
	Target         string            `json:"target"`
	TransportCfg   transport.Config  `json:"transport_config"`
	Load           Config            `json:"load"`
	ResponseBytes  int               `json:"response_bytes"`
	Start          time.Time         `json:"start"`
	End            time.Time         `json:"end"`
	HandshakeUs    int64             `json:"handshake_us"`
	Dials          int64             `json:"dials"`
	Total          int64             `json:"total"`
	OK             int64             `json:"ok"`
	Errors         map[string]int64  `json:"errors"`
	LateStarts     int64             `json:"late_starts"`
	Unsent         int64             `json:"unsent"`
	RecorderStalls int64             `json:"recorder_stalls"`
	Sysctl         map[string]string `json:"sysctl,omitempty"` // net.core.rmem_max / wmem_max
}
func WriteMeta(path string, m Meta) error
func ReadSysctls() map[string]string // missing /proc entries are skipped
// cmd/loadgen
func run(ctx context.Context, args []string, log *slog.Logger) error
```

- [ ] **1. Testes que falham**

| Teste | Esperado |
|---|---|
| `TestWriteMeta` | JSON gravado tem as chaves `dials`, `errors`, `transport`, `handshake_us`, `load` |
| `TestReadSysctls` | não entra em pânico sem `/proc` (Windows) |
| `TestSmoke` (h2 e h3) | servidor in-process (`transport.Listen` + handler do bench) + `run` com `--requests=1000 --warmup=0 --concurrency=50 --strict`: retorna nil; o CSV tem o header exato + 1000 linhas `ok`/`measure`; `meta.json` com `dials=1`, `ok=1000` e `transport` certo |
| `TestTransportMismatch` | `--transport=h3` contra servidor h2, `--handshake-timeout=500ms` → erro em < 3 s, sem CSV com linhas |
| `TestBadOut` | `--out` apontando para um caminho cujo diretório é arquivo → erro antes de qualquer RPC |

- [ ] **2. Implementação de `run`, em ordem**

1. Flags da §5.4 + `RegisterFlags(fs, RoleClient)`.
2. `Validate` do transporte e do runner; `--target` via `SplitHostPort`; `0 ≤ response-bytes ≤ MaxResponseBytes`.
3. Cria o diretório e o arquivo de `--out`.
4. Monta os clientes:
   ```go
   cli, err := transport.NewClient(tcfg)
   bench := benchv1connect.NewBenchServiceClient(cli, "https://"+target,
       connect.WithGRPC(), connect.WithAcceptCompression("gzip", nil, nil))
   ```
5. RPC de teste com deadline `HandshakeTimeout + Deadline` → `handshake_us`. Aplica `VerifyServedProto(resp.Header())`; em caso de erro, aborta sempre.
6. `NewRecorder(f, 1<<16)` → `Run` → `rec.Close()` → `f.Close()`.
7. `WriteMeta(dir(out)/meta.json)`.
8. Log `summary` com `total`, `ok`, `errors`, `dials`, `late_starts`, `unsent`.
9. Com `--strict`, retorna erro se `OK != Total` ou se `Dials() != 1`.

A requisição (payload aleatório de `--payload-bytes`) é montada uma vez e reaproveitada por todos os workers.

- [ ] **3. Rode** `go test ./...` e `go vet ./...`. Esperado: tudo PASS. Isto conclui a etapa 3 (smoke local).
- [ ] **4. Commit (você):** `Add load generator with metadata`

---

### Tarefa 10: Docker, compose, certificados e Makefile (etapa 4)

**Arquivos:** criar `.dockerignore`, `deploy/Dockerfile`, `deploy/compose.yaml`, `scripts/gen-certs.sh`, `Makefile`.

- [ ] **1. `scripts/gen-certs.sh`** (bash, idempotente)

Se `certs/server.crt` já existir, avisa e sai com 0. Senão, gera a CA e o certificado do servidor com openssl:
- CA: `openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 -subj /CN=bench-ca` (o `v3_ca` padrão do openssl dá `CA:TRUE`).
- Servidor: chave e CSR P-256 com `/CN=server`, assinados com `openssl x509 -req -CA -CAkey -CAcreateserial -days 825 -extfile certs/server.ext`.
- `server.ext`:
  ```
  subjectAltName=DNS:server,DNS:localhost,IP:127.0.0.1
  extendedKeyUsage=serverAuth
  keyUsage=critical,digitalSignature
  ```
- Saída: `certs/{ca.crt,server.crt,server.key}`.

- [ ] **2. `deploy/Dockerfile`**
```dockerfile
FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/ ./cmd/...

FROM debian:trixie-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 && rm -rf /var/lib/apt/lists/*

FROM runtime AS server
COPY --from=build /out/server /usr/local/bin/server
ENTRYPOINT ["/usr/local/bin/server"]

FROM runtime AS loadgen
COPY --from=build /out/loadgen /usr/local/bin/loadgen
ENTRYPOINT ["/usr/local/bin/loadgen"]
```
`.dockerignore`: `.git`, `results`, `certs`, `doc`, `analysis`, `.venv`.

Os contêineres rodam como root, porque o `tc` da Fase 2 precisa de `NET_ADMIN` efetivo.

- [ ] **3. `deploy/compose.yaml`**

- `name: bench`; rede `benchnet` (bridge).
- `server`:
  - build no target `server` com `args.VERSION`; imagem `bench-server:local`;
  - `expose: [8443/tcp, 8443/udp]`;
  - env: `TRANSPORT=${TRANSPORT:-h2}`, `ADDR=:8443`, `TLS_CERT`/`TLS_KEY`/`TLS_CA` em `/certs`;
  - `cap_add: [NET_ADMIN]`, `cpuset: ${SERVER_CPUS:-2-3}`;
  - volume `../certs:/certs:ro`;
  - healthcheck `["CMD","/usr/local/bin/server","healthcheck"]` com `start_period: 10s`, `start_interval: 1s`, `interval: 60s`, `timeout: 3s`, `retries: 3`.
- `loadgen`:
  - target `loadgen`; imagem `bench-loadgen:local`;
  - env `TRANSPORT`, `TLS_CA`;
  - `cap_add: [NET_ADMIN]`, `cpuset: ${LOADGEN_CPUS:-4-5}`;
  - volumes `../certs:/certs:ro` e `../results:/results`;
  - `depends_on: {server: {condition: service_healthy}}`;
  - `profiles: [load]` (o `compose run loadgen` ativa o profile).

- [ ] **4. `Makefile`**
```make
COMPOSE   := docker compose -f deploy/compose.yaml
VERSION   ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
TRANSPORT ?= h2
export VERSION TRANSPORT

.PHONY: certs build test smoke down
certs:
	bash scripts/gen-certs.sh
build:
	$(COMPOSE) build
test:
	docker run --rm -v "$(CURDIR)":/src -v bench-gomod:/go/pkg/mod -w /src golang:1.27.1-trixie go test -race ./...
smoke:
	$(COMPOSE) up -d --wait server
	$(COMPOSE) run --rm loadgen --target=server:8443 --requests=1000 --warmup=0 --strict \
		--out=/results/smoke/$(TRANSPORT)/requests.csv; rc=$$?; $(COMPOSE) down; exit $$rc
down:
	$(COMPOSE) down --remove-orphans
```

- [ ] **5. Verificação local:** `go build ./...` e `go test ./...` (nada de Docker no Windows). A verificação real é na VM, na seção "Fim da Fase 1".
- [ ] **6. Commit (você):** `Add Docker, compose, certs and Makefile`

---

### Fim da Fase 1

- [ ] `superpowers:verification-before-completion`: mostrar a saída real de `go vet ./...` e `go test ./...` no Windows.
- [ ] `superpowers:requesting-code-review` da Fase 1 inteira; corrigir o que for aceito.
- [ ] Entregar ao usuário os comandos da VM e o resultado esperado de cada um. Rascunho:

```bash
sudo apt-get install -y make openssl        # once; openssl is usually present
docker version --format '{{.Server.Version}}' # expect >= 25 (start_interval)
docker compose version                        # expect v2.20+
git pull
make certs                                    # certs/ca.crt, server.crt, server.key
make build                                    # bench-server:local and bench-loadgen:local
make test                                     # ok for every package (-race)
make smoke TRANSPORT=h2                       # summary: ok=1000 total=1000 dials=1; exit 0
make smoke TRANSPORT=h3                       # same, transport h3
TRANSPORT=h2 docker compose -f deploy/compose.yaml up -d --wait server
docker compose -f deploy/compose.yaml exec server ss -tuln   # tcp LISTEN :8443, no udp :8443
make down
TRANSPORT=h3 docker compose -f deploy/compose.yaml up -d --wait server
docker compose -f deploy/compose.yaml exec server ss -tuln   # udp UNCONN :8443, no tcp :8443
make down
```
- [ ] **Parar** até o usuário confirmar os critérios da §5.5 na VM.

---

# Fase 2 — etapas 5 a 7

### Tarefa 11: `netem.sh`, entrypoint, host tuning e cenários (etapa 5)

**Arquivos:**
- Criar: `deploy/netem.sh`, `scripts/host-tuning.sh`, `experiments/scenarios/{s1-baseline,s2-loss-1,s2-loss-3,s2-loss-5,s3-jitter,s5-outage}.env`
- Modificar: `deploy/Dockerfile` (`COPY --chmod=755 deploy/netem.sh /usr/local/bin/netem.sh`; `ENTRYPOINT ["netem.sh","run","/usr/local/bin/server"]` e o equivalente no loadgen) e `deploy/compose.yaml` (`NETEM_*` nos dois serviços)

- [ ] **1. `deploy/netem.sh`** (POSIX sh; `IFACE=${NETEM_IFACE:-eth0}`)
  - `apply`: monta a lista de argumentos com `delay ${NETEM_DELAY}ms [${NETEM_JITTER}ms distribution ${NETEM_DIST:-normal}]` e `loss ${NETEM_LOSS}%`, só para os valores diferentes de 0. Com lista vazia, remove a qdisc. Senão, `tc qdisc replace dev $IFACE root netem …`.
  - `clear`: `tc qdisc del dev $IFACE root 2>/dev/null || true`.
  - `outage <s>`: imprime `event,ts_ns` e `outage_start,$(date +%s%N)`; aplica `loss 100%`; dorme `<s>`; roda `apply` e imprime `outage_end,…`.
  - `run <cmd…>`: `apply && exec "$@"`.
- [ ] **2. `scripts/host-tuning.sh`** (bash, `sudo`): `sysctl -w net.core.rmem_max=7500000 net.core.wmem_max=7500000`. `modprobe sch_netem`; se falhar, sai com a mensagem "install linux-modules-extra-$(uname -r)".
- [ ] **3. Cenários** (valores da §6.3). Cada `.env` tem `NETEM_DELAY`, `NETEM_JITTER`, `NETEM_LOSS` e `LOADGEN_ARGS` (padrão `--concurrency=50 --duration=60s --warmup=10s`). O `s5-outage` acrescenta `OUTAGE_AT=30` e `OUTAGE_SECS=5`.
- [ ] **4. Verificação na VM:**
  - `make build`;
  - `NETEM_LOSS=3 TRANSPORT=h2 docker compose -f deploy/compose.yaml up -d --wait server`;
  - `docker compose -f deploy/compose.yaml exec server tc qdisc show dev eth0` → `netem … loss 3%`.
- [ ] **5. Commit (você):** `Add netem fault injection and scenarios`

---

### Tarefa 12: runner `scripts/run.sh` (etapa 5)

**Arquivos:** criar `scripts/run.sh`; modificar `Makefile` (`run-all`).

- [ ] **1. Estrutura**

`run.sh` tem dois modos:
- sem argumentos: roda a matriz;
- `run.sh rep <env> <transport> <dir>`: roda uma repetição num processo próprio. Isso é necessário porque o `set -e` do bash é ignorado dentro de `if ! func`, então cada repetição falha de forma isolada.

**Matriz:**
- `run_id=$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short HEAD)`;
- `for env in experiments/scenarios/${SCENARIOS:-*}.env; for rep in 1..REPS`, com a ordem `h2 h3` nas repetições ímpares e `h3 h2` nas pares;
- se a repetição falhar, grava `scenario,transport,rep` em `results/<run_id>/failures.log` e segue;
- no início, avisa se `rmem_max < 7500000`;
- imprime o `run_id` no fim.

**Repetição:**
- `set -euo pipefail`; carrega o `.env` com `set -a; . env; set +a`;
- `trap` que sempre faz `compose down --remove-orphans` e mata o amostrador;
- sobe o servidor com `compose up -d --wait server`;
- amostrador de `docker stats` em loop, a cada 1 s: `ts_ns,name,cpu_perc,mem_usage` → `stats.csv`;
- se houver `OUTAGE_AT`, em background: `sleep $OUTAGE_AT` e então `compose exec -T server netem.sh outage $OUTAGE_SECS > events.csv` (apagão só no servidor, como na §6.4 do spec);
- `compose run --rm --name bench-loadgen loadgen $LOADGEN_ARGS --target=server:8443 --out=/results/<rel>/requests.csv > loadgen.log 2>&1`;
- `compose logs --no-color server > server.log`.

- [ ] **2. Makefile:** `run-all: ; REPS=$(REPS) bash scripts/run.sh` (`REPS ?= 5`). O modo `rep` é chamado como `bash "$0" rep …`.
- [ ] **3. Verificação na VM:** `SCENARIOS=s1-baseline REPS=1 make run-all` → `results/<id>/s1-baseline/{h2,h3}/rep-1/` com `requests.csv`, `meta.json`, `stats.csv`, `server.log` e `loadgen.log`. `SCENARIOS=s5-outage REPS=1 make run-all` → `events.csv` com `outage_start` e `outage_end`.
- [ ] **4. Commit (você):** `Add experiment runner`

---

### Tarefa 13: análise — funções de métrica (etapa 6)

**Arquivos:** criar `analysis/requirements.txt`, `analysis/pytest.ini` (`[pytest]` + `pythonpath = .`), `analysis/analyze.py` (só as funções) e `analysis/tests/test_metrics.py`.

**Produz** (em `analyze.py`):
```python
def load_requests(path) -> pd.DataFrame            # measure rows only
def latency_stats(df) -> dict                      # p50,p90,p99,p999,mean,std (us), status == "ok"
def throughput(df) -> dict                         # ok_rps, mb_s over [min start, max(start + latency)]
def error_rate(df) -> float
def throughput_series(df, bin_s=1.0) -> pd.Series  # ok completions per bin, index = bin start ns
def recovery_time(series, outage_start_ns, outage_end_ns, threshold=0.9, windows=3) -> float | None
def parse_stats(path) -> pd.DataFrame              # cpu_perc float, mem_mib float
```
`requirements.txt`: `pandas==3.0.6`, `numpy==2.5.3`, `matplotlib==3.11.2`, `scipy==1.18.1`, `pytest==9.1.1`.

- [ ] **1. Testes que falham**

| Teste | Esperado |
|---|---|
| percentis | latências 1..1000 us `ok` + 10 linhas `unavailable` → `p50=500.5`, `p99=990.01`, `p999=999.001` (numpy linear); as não-ok ignoradas |
| vazão | 100 linhas `ok`, início a cada 10 ms, latência 10 ms, 1000 B → `ok_rps == 100.0`, `mb_s == 0.1` |
| taxa de erro | 3 de 10 não-ok → `0.3` |
| recuperação | bins de 1 s: 100/s até 30 s, 0 de 30 a 35 s, 50 em 35 s, 95 depois; `outage_end=35s` → `1.0` |
| recuperação limite | 90 exatos contam; nunca volta → `None` |
| `parse_stats` | `12.5%` → 12.5; `100MiB / 7.6GiB` → 100.0; `1.5GiB / …` → 1536.0; `512KiB / …` → 0.5 |

- [ ] **2. Implementação mínima** das funções.
- [ ] **3. Rode** no Windows, em venv:
  ```bash
  python -m venv .venv
  .venv/Scripts/pip install -r analysis/requirements.txt
  .venv/Scripts/python -m pytest analysis
  ```
  Esperado: PASS.
- [ ] **4. Commit (você):** `Add analysis metrics`

---

### Tarefa 14: análise — resumo, estatística, gráficos e contêiner (etapa 6)

**Arquivos:** modificar `analysis/analyze.py` e `analysis/tests/test_metrics.py`, `deploy/Dockerfile` (target `analysis`), `deploy/compose.yaml` (serviço `analysis`, profile `analysis`) e `Makefile` (`analyze`, `test-py`).

**Produz:**
```python
def summarize(reps: pd.DataFrame) -> pd.DataFrame  # one row per scenario x transport: <metric>_mean, <metric>_ci95
def main(argv: list[str]) -> int                   # analyze.py <run_dir> -> <run_dir>/analysis/
```

- [ ] **1. Teste que falha:** `summarize` com 2 transportes × 3 repetições sintéticas → `_mean` e `_ci95` para cada métrica (`p50`, `p90`, `p99`, `p999`, `ok_rps`, `mb_s`, `error_rate`, `cpu`, `mem_mib` e, no s5, `recovery_s`). O teste confere `p99_mean`, `p99_ci95` (t de Student, `n-1` graus de liberdade), `cv_p50` e `mwu_p`. O `mwu_p` é o Mann-Whitney U bilateral, h2 vs h3, sobre o p99 por repetição, igual nas duas linhas do cenário. Valores conferidos contra `scipy.stats` à mão.
- [ ] **2. Implementação**

- `main` percorre `<run>/<scenario>/<transport>/rep-*` e monta uma linha por repetição: `latency_stats`, `throughput`, `error_rate`, CPU e memória médias do `stats.csv` e, no s5, `recovery_time` a partir do `events.csv`.
- Grava `summary.csv` e três PNGs: CDF de latência por cenário (h2 vs h3); boxplot do p99 por repetição; série de vazão do s5 com o apagão sombreado.
- Usa `matplotlib.use("Agg")`. Carregar a skill `dataviz` antes de escrever os gráficos.

- [ ] **3. Contêiner**

Dockerfile:
```dockerfile
FROM python:3.13-slim-trixie AS analysis
COPY analysis/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY analysis /app/analysis
WORKDIR /app/analysis
ENTRYPOINT ["python", "analyze.py"]
```
Tirar `analysis` do `.dockerignore`.

Makefile:
- `analyze: ; $(COMPOSE) run --rm analysis /results/$(RUN)`;
- `test-py: ; $(COMPOSE) run --rm --entrypoint python analysis -m pytest`.

- [ ] **4. Rode** `pytest` no Windows (PASS). Na VM, `make test-py` (PASS) e `make analyze RUN=<id da Tarefa 12>` → `summary.csv` + 3 PNGs.
- [ ] **5. Commit (você):** `Add summary, statistics and plots`

---

### Tarefa 15: rodada completa e calibração (etapa 7)

- [ ] **1. Na VM:** `sudo bash scripts/host-tuning.sh`, depois `make run-all REPS=5` (≈ 6 cenários × 2 × 5 × ~80 s ≈ 80 min) e `make analyze RUN=<id>`.
- [ ] **2. Conferir os critérios da §6.6**
  - nenhuma linha em `failures.log`;
  - os arquivos de cada repetição completos;
  - p99 do h2 crescendo de 1% → 5% de perda;
  - s5 com queda e retomada, e `recovery_s` calculado;
  - `cv_p50 < 0.10`;
  - `dials == 1` em todo `meta.json`.
- [ ] **3. Calibração:** se algum critério falhar, ajustar o delay, a duração ou o `--rps` nos `.env` e rodar de novo só o subconjunto (`SCENARIOS='s2-loss-*'`). Registrar os valores finais na §6.3 do spec.
- [ ] **4.** `superpowers:verification-before-completion` e `superpowers:requesting-code-review` da Fase 2.
- [ ] **5. Commit (você):** `Calibrate scenarios after full run`
