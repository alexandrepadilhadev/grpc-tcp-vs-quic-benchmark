# Plano do POC — gRPC sobre HTTP/2 (TCP) vs HTTP/3 (QUIC)

> Status: **rev. 2 — aprovado** · Última revisão: 2026-09-27
> Contexto: TCC "Desempenho do gRPC sobre TCP e QUIC" (Alexandre Padilha, UTFPR)
> Rev. 2: ajustes após o levantamento técnico (HTTP/2 nativo no Go 1.27, APIs do quic-go v0.63, semântica do `--rps`, health check em h3, versões fixadas).

## 1. Objetivo

Entregar um POC mínimo, executável e testável que:

1. **Fase 1 — Implementação:** um cliente e um servidor gRPC básicos que trocam de protocolo de transporte (`h2` ↔ `h3`) por configuração, provando o conceito.
2. **Fase 2 — Ambiente de teste:** infraestrutura Docker, geração de carga, injeção de falhas de rede, coleta de logs/métricas e análise de resultados sobre esse cliente/servidor.

**Requisito estrutural:** o POC é o *esqueleto* do sistema final do TCC. Depois, só a pasta de serviços é substituída (Gateway, Core, Antifraude, Ledger). Transporte, gerador de carga, rede, runner e análise são reaproveitados sem mudança de contrato.

## 2. Decisões tomadas

| Tema | Decisão | Motivo |
|---|---|---|
| Linguagem dos serviços e do loadgen | **Go** | Uma única implementação de QUIC (quic-go) e de TLS (crypto/tls) no mesmo runtime, usada nas duas pontas: a comparação fica isolada no transporte. Binário único e imagens pequenas. |
| Biblioteca RPC | **ConnectRPC (`connect-go`) falando o protocolo gRPC** | O `grpc-go` oficial não suporta HTTP/3. O `connect-go` é wire-compatible com gRPC e roda sobre `net/http` (h2) e `quic-go/http3` (h3). É a mesma base de McDonald (2024). |
| Pilha HTTP/2 | **`net/http` nativo do Go 1.27** (`Protocols` só HTTP/2 + `http.HTTP2Config`) | No Go 1.27 a implementação de referência do HTTP/2 foi para a stdlib. O `x/net/http2` virou um wrapper dela e só recebe correções críticas. |
| Linguagem da análise | **Python** (pandas, matplotlib, scipy) | Melhor ecossistema para estatística e gráficos. Não entra no caminho medido. |
| Orquestração | **Docker Compose** + `Makefile` + `run.sh` | É o mínimo reprodutível. Kubernetes/kind fica fora do POC. |
| Ambiente | Desenvolvimento no **Windows** (só `go build`, `go test` e `pytest`); Docker, `tc` e scripts numa **VM Linux** | Tudo que roda na VM é POSIX com fim de linha LF (`.gitattributes`). |
| Geração de código | **`buf` + plugins fixados em `tools/go.mod`**; `gen/` versionado | A VM não precisa de `buf`/`protoc`, e as versões ficam reprodutíveis. |
| Injeção de falhas | **`tc netem`** dentro dos contêineres (`NET_ADMIN`) | O host é Linux nativo, então o netem funciona direto sem VM intermediária. |
| Modelo de carga | **Concorrência fixa (loop fechado)** + flag opcional `--rps` (taxa fixa) | O loop fechado é o mais simples. A taxa fixa evita a *coordinated omission* nos cenários com perda. |
| Latência sob `--rps` | Medida a partir do horário **planejado** de cada RPC (estilo wrk2) | É o que corrige a *coordinated omission*: a espera por um worker livre entra na latência. |
| Keepalive e timeouts | Sem keepalive de aplicação nos dois; idle 30 s e handshake 5 s nos dois | O s5 mede a recuperação do transporte na mesma conexão (RTO do TCP vs PTO do QUIC), não uma reconexão. |
| Compressão | **Desligada** nas duas pontas; payload aleatório | Evita que o gzip entre na medição. |
| Métricas / logs | **CSV por requisição + JSON de metadados + `docker stats`**, sem Prometheus/Grafana | É o mínimo que garante análise offline e reprodutível. |
| Cenários no POC | 1 (baseline), 2 (perda 1/3/5%), 3 (jitter) + **apagão** para medir o tempo de recuperação | O Cenário 4 (migração de IP) foi **adiado**. |

## 3. Arquitetura mínima

```
                    Docker network "benchnet" (bridge)
 ┌──────────────────────────┐                      ┌──────────────────────────┐
 │ loadgen                  │   gRPC (h2 | h3)     │ server                   │
 │  - 1 conexão multiplexada│ ───────────────────► │  - BenchService.Unary    │
 │  - N requisições em voo  │   TLS 1.3 em ambos   │  - --transport=h2|h3     │
 │  - CSV por requisição    │                      │  - logs JSON (slog)      │
 │ netem.sh (eth0 egress)   │                      │ netem.sh (eth0 egress)   │
 └────────────┬─────────────┘                      └──────────────────────────┘
              │ volume ./results
              ▼
   results/<run_id>/<scenario>/<transport>/rep-<n>/{requests.csv, meta.json, stats.csv, *.log}
              │
              ▼
   analysis/analyze.py ──► summary.csv + gráficos + testes estatísticos
```

Pontos essenciais para o experimento ser válido:

- **Uma única conexão com N streams simultâneos.** Sem multiplexação em uma conexão, o HoLB do TCP não aparece. O transporte força isso nos dois modos (`StrictMaxConcurrentRequests` no h2; bloqueio em `OpenStreamSync` no QUIC), e o loadgen registra o número de conexões abertas (`dials`) no `meta.json`.
- **TLS 1.3 nos dois modos**, com o mesmo certificado. A única variável é TCP+HTTP/2 vs QUIC+HTTP/3.
- **Parâmetros simétricos:** limite de streams, handshake timeout, idle timeout e keepalive iguais nas duas pilhas (tabela em 5.2).
- **Verificação do protocolo efetivo:** o servidor devolve o header `x-served-proto` (`r.Proto`) em toda resposta, o dialer do cliente rejeita ALPN inesperado e o loadgen aborta se o protocolo não bater. Assim não há fallback silencioso.
- **Isolamento de CPU:** `cpuset` distintos para o loadgen e o servidor no compose, para reduzir o ruído de estarem no mesmo host.

## 4. Estrutura do repositório

```
.
├── .gitattributes                     # eol=lf para o que roda na VM
├── .gitignore
├── go.mod / go.sum
├── buf.yaml / buf.gen.yaml            # geração com plugins locais fixados
├── tools/go.mod                       # buf, protoc-gen-go, protoc-gen-connect-go (diretiva tool)
├── proto/bench/v1/bench.proto         # contrato do POC
├── gen/                               # código gerado (versionado)
├── cmd/
│   ├── server/main.go                 # servidor do POC + subcomando healthcheck (substituído depois)
│   └── loadgen/main.go                # gerador de carga (reaproveitado)
├── internal/
│   ├── transport/                     # ★ núcleo reaproveitável
│   │   ├── transport.go               # Transport (h2|h3), Parse, erros tipados
│   │   ├── config.go                  # Config, Role, flags com fallback para env, validação
│   │   ├── tls.go                     # carga de cert/CA, TLS 1.3 mínimo, ALPN
│   │   ├── server.go                  # Listen + Serve: h2 (TCP) ou h3 (UDP), x-served-proto
│   │   ├── client.go                  # NewClient: uma conexão multiplexada, contador de dials
│   │   └── transporttest/             # CA + cert efêmeros para testes
│   ├── benchsvc/                      # implementação do BenchService (substituída depois)
│   └── loadgen/
│       ├── runner.go                  # workers, --concurrency, --rps, --duration, --requests, deadline
│       ├── recorder.go                # escrita do CSV com buffer, flush seguro
│       └── meta.go                    # meta.json
├── deploy/
│   ├── Dockerfile                     # multi-stage: targets server, loadgen e analysis
│   ├── compose.yaml                   # server + loadgen + analysis (profile) + rede
│   └── netem.sh                       # aplica/limpa tc qdisc a partir de env
├── experiments/
│   └── scenarios/                     # um .env por cenário
│       ├── s1-baseline.env
│       ├── s2-loss-1.env  s2-loss-3.env  s2-loss-5.env
│       ├── s3-jitter.env
│       └── s5-outage.env
├── scripts/
│   ├── gen-certs.sh                   # CA + cert do servidor (openssl)
│   ├── run.sh                         # matriz cenário × transporte × repetição
│   └── host-tuning.sh                 # sysctl de buffers UDP (quic-go), roda na VM
├── analysis/
│   ├── requirements.txt
│   ├── analyze.py                     # métricas, estatística, gráficos
│   └── tests/                         # pytest das funções de métrica
├── results/                           # (gitignored)
├── doc/plan/poc-plan.md
└── Makefile
```

## 5. Fase 1 — Implementação (cliente e servidor básicos)

### 5.1 Contrato (`bench.proto`)

```proto
syntax = "proto3";
package bench.v1;

service BenchService {
  rpc Unary(UnaryRequest) returns (UnaryResponse);
}

message UnaryRequest {
  bytes  payload          = 1; // corpo de tamanho configurável
  uint32 response_size    = 2; // bytes pedidos na resposta
  uint32 server_delay_us  = 3; // atraso simulado de processamento
}

message UnaryResponse {
  bytes payload = 1;
}
```

Só *unary* no POC. *Streaming* fica para depois, se o sistema final precisar.

### 5.2 Pacote `internal/transport` (a peça reaproveitável)

API pública. O pacote não importa o connect: depende só de `net/http`, `quic-go` e `log/slog`.

```go
package transport

type Transport string

const (
	H2 Transport = "h2" // HTTP/2 over TLS 1.3 / TCP
	H3 Transport = "h3" // HTTP/3 over QUIC / UDP
)

func Parse(s string) (Transport, error) // wraps ErrInvalidTransport
func (t Transport) Network() string     // "tcp" | "udp"
func (t Transport) ALPN() string        // "h2" | "h3"
func (t Transport) Proto() string       // "HTTP/2.0" | "HTTP/3.0"
func (t Transport) String() string
func (t *Transport) Set(s string) error // flag.Value

var (
	ErrInvalidTransport = errors.New("transport: invalid transport")
	ErrInvalidConfig    = errors.New("transport: invalid config")
	ErrTLSConfig        = errors.New("transport: tls config")
	ErrProtoMismatch    = errors.New("transport: protocol mismatch")
)

// Role selects which Config fields are registered and validated.
type Role uint8

const (
	RoleServer Role = 1 << iota
	RoleClient
)

// Config is shared by a service's server and all of its clients,
// so a single TRANSPORT switches the whole stack.
type Config struct {
	Transport            Transport     // both   --transport         TRANSPORT              h2
	HandshakeTimeout     time.Duration // both   --handshake-timeout HANDSHAKE_TIMEOUT      5s
	IdleTimeout          time.Duration // both   --idle-timeout      IDLE_TIMEOUT           30s
	Addr                 string        // server --addr              ADDR                   :8443
	CertFile             string        // server --tls-cert          TLS_CERT
	KeyFile              string        // server --tls-key           TLS_KEY
	MaxConcurrentStreams uint32        // server --max-streams       MAX_CONCURRENT_STREAMS 1000
	ShutdownTimeout      time.Duration // server --shutdown-timeout  SHUTDOWN_TIMEOUT       10s
	CAFile               string        // client --tls-ca            TLS_CA
}

func DefaultConfig() Config

// RegisterFlags binds the fields of the given roles; precedence: flag > env > default.
func (c *Config) RegisterFlags(fs *flag.FlagSet, r Role)

// Validate checks the fields of the given roles; errors wrap ErrInvalidConfig.
func (c Config) Validate(r Role) error

const HeaderServedProto = "x-served-proto"

type Server struct{ /* unexported */ }

// Listen validates cfg, loads TLS and binds the TCP or UDP socket.
// Every response carries HeaderServedProto.
func Listen(cfg Config, h http.Handler, log *slog.Logger) (*Server, error)
func (s *Server) Addr() net.Addr

// Serve blocks until ctx is done, then shuts down gracefully within ShutdownTimeout.
func (s *Server) Serve(ctx context.Context) error

// Client uses one multiplexed connection per host; it satisfies connect.HTTPClient.
type Client struct{ /* unexported */ }

func NewClient(cfg Config) (*Client, error)
func (c *Client) Do(req *http.Request) (*http.Response, error)
func (c *Client) Dials() int64 // connections opened; a valid run has exactly 1
func (c *Client) Close() error

func VerifyServedProto(t Transport, h http.Header) error // wraps ErrProtoMismatch
```

- **`Config` único por serviço:** o servidor e todos os clientes de um serviço usam o mesmo `TRANSPORT`. O `Role` diz quais campos cada binário registra e valida: o loadgen usa `RoleClient`; o servidor do POC usa `RoleServer|RoleClient`, porque o subcomando `healthcheck` é um cliente.
- **Servidor:** o `Listen` valida a config, carrega o TLS e abre o socket. Um erro de bind aparece na inicialização e, com porta 0, os testes obtêm o endereço real. O `Serve` bloqueia até o `ctx` terminar (SIGTERM via `signal.NotifyContext`) e faz o *graceful shutdown*.
  - `h2`: `http.Server` com `Protocols` só HTTP/2, `HTTP2: &http.HTTP2Config{MaxConcurrentStreams}` e `tls.Config{MinVersion: TLS13, NextProtos: ["h2"]}`, escutando em TCP.
  - `h3`: `http3.Server` do quic-go com o mesmo certificado (`NextProtos: ["h3"]`), servindo um socket UDP aberto pelo `Listen` via `Serve(conn)`. O quic-go documenta uma corrida entre `Shutdown` e `ListenAndServe` na criação do socket.
  - O mesmo `http.Handler` (ConnectRPC) serve os dois, e toda resposta leva o `x-served-proto`.
- **Cliente:** `*Client` satisfaz `connect.HTTPClient`. O cliente Connect é criado com `connect.WithGRPC()`, forçando o protocolo gRPC.
  - `h2`: `http.Transport` com `Protocols` só HTTP/2 e `HTTP2: &http.HTTP2Config{StrictMaxConcurrentRequests: true}`. Sem isso, ao atingir o limite de streams o transporte abre uma segunda conexão.
  - `h3`: `http3.Transport` do quic-go, que já deduplica a discagem por host e bloqueia em `OpenStreamSync` ao atingir o limite.
  - Os dois dialers contam as conexões (`Dials()`) e rejeitam ALPN diferente do esperado (`ErrProtoMismatch`).
- Erros tipados (`ErrInvalidTransport`, `ErrInvalidConfig`, `ErrTLSConfig`, `ErrProtoMismatch`), usados com `errors.Is`, e mensagens curtas.
- `transporttest.NewCertFiles(t)`: CA + cert efêmeros em `t.TempDir()`, para os testes do pacote, do loadgen e, depois, dos serviços.

Parâmetros simétricos:

| Parâmetro | Default | h2 (`net/http`) | h3 (`quic-go`) |
|---|---|---|---|
| Limite de streams | 1000 | `HTTP2Config.MaxConcurrentStreams` | `quic.Config.MaxIncomingStreams` (default do quic-go: 100) |
| Excesso de streams | bloqueia | `StrictMaxConcurrentRequests: true` | `OpenStreamSync` (padrão) |
| Handshake timeout | 5 s | cliente: timeout no `DialContext` (TCP + TLS); servidor: `ReadHeaderTimeout` | `HandshakeIdleTimeout` |
| Idle timeout | 30 s | `http.Server.IdleTimeout` / `http.Transport.IdleConnTimeout` | `MaxIdleTimeout` |
| Keepalive | desligado | `SendPingTimeout: 0` | `KeepAlivePeriod: 0` |

### 5.3 Servidor (`cmd/server`)

- Registra o `BenchService`, aplica `server_delay_us` e devolve `response_size` bytes de um payload aleatório pré-gerado.
- O header `x-served-proto: HTTP/2.0 | HTTP/3.0` vem do `transport` (5.2).
- Compressão desligada: o gzip não é registrado no handler.
- Logs estruturados (`log/slog`, JSON): inicialização, configuração efetiva e erros.
- **Log por RPC** (`internal/rpclog`, interceptor do connect): uma linha por RPC, com sucesso ou falha, contendo `procedure`, `code`, `duration_us`, `peer` e `protocol`.
  - Sai no nível `debug`. O padrão é `--log-level`/`LOG_LEVEL=info`, então fica desligado nas medições e não as influencia.
  - É a base do log dos microsserviços da próxima fase.
- Health check: o subcomando `server healthcheck` usa o `transport.NewClient` para fazer `GET https://localhost:<porta do --addr>/healthz` no transporte configurado e sai com 0 ou 1. Prova que o socket TCP ou UDP real está no ar. Roda em `lo`, fora do netem.

### 5.4 Loadgen (`cmd/loadgen`)

Flags principais:

| Flag | Default | Descrição |
|---|---|---|
| `--transport` | `h2` | `h2` ou `h3` |
| `--target` | `server:8443` | endereço |
| `--concurrency` | `50` | requisições em voo (workers) |
| `--rps` | `0` | 0 = loop fechado; >0 = taxa fixa global |
| `--duration` | `60s` | janela medida |
| `--requests` | `0` | >0 = para após N RPCs medidas (ignora `--duration`) |
| `--warmup` | `10s` | descartado na análise (marcado no CSV) |
| `--payload-bytes` / `--response-bytes` | `1024` | tamanhos |
| `--deadline` | `2s` | timeout por RPC (vira erro `DeadlineExceeded`) |
| `--strict` | `false` | sai com código 1 em qualquer RPC ≠ `ok` |
| `--out` | `/results/requests.csv` | saída |

Mais as flags de cliente do `transport.Config` (`--tls-ca`, `--handshake-timeout`, `--idle-timeout`).

Comportamento:

- **Validação na inicialização:** uma RPC de teste confere o `x-served-proto` e registra o tempo até a primeira resposta (inclui o handshake). Divergência de protocolo sempre aborta, com ou sem `--strict`.
- **Tempo:** `start_ns` em Unix ns (relógio de parede no início do run + tempo monotônico decorrido). As durações usam o relógio monotônico.
  - Loop fechado: `start_ns` é o instante do envio, e `latency_us` vai do envio à resposta.
  - `--rps` > 0: agenda global `t_k = t0 + k/rps`, distribuída entre os `--concurrency` workers. `start_ns = t_k`, e `latency_us` é medida desde `t_k`, então a espera por um worker livre conta. Nenhum slot é descartado: depois de um apagão, as RPCs atrasadas saem em sequência. O `meta.json` registra `late_starts` como sinal de saturação do loadgen.
- Grava uma linha por requisição de forma assíncrona (canal com buffer de 64k + writer único) para não bloquear os workers. Com o canal cheio, o worker espera (nenhuma linha é descartada) e o `meta.json` conta `recorder_stalls`.
- Compressão desligada: o cliente não anuncia gzip.
- Ao terminar, grava o `meta.json` com:
  - a configuração e os parâmetros efetivos do transporte (tabela em 5.2);
  - o horário de início/fim;
  - a versão (`git describe --always --dirty`, injetada via `-ldflags`);
  - o tempo do handshake e o número de conexões abertas (`dials`);
  - o contador de erros por código, `late_starts` e `recorder_stalls`;
  - `net.core.rmem_max`/`wmem_max` lidos de `/proc`, quando existirem.

**Schema do `requests.csv`** (contrato estável, a análise depende dele):

```
seq,worker,start_ns,latency_us,status,req_bytes,resp_bytes,phase
```

- `status`: código gRPC (`ok`, `deadline_exceeded`, `unavailable`, …).
- `phase`: `warmup` | `measure`.

### 5.5 Critérios de aceite da Fase 1

- [ ] `make certs build` gera os certificados e as imagens (com os binários) sem erro.
- [ ] `make smoke TRANSPORT=h2` e `make smoke TRANSPORT=h3` completam 1000 RPCs sem erro (`--requests 1000 --warmup 0 --strict`).
- [ ] O `x-served-proto` confirma HTTP/2.0 e HTTP/3.0, respectivamente, e o `meta.json` registra `dials = 1`.
- [ ] `ss -tn` / `ss -un` no contêiner mostra TCP no h2 e UDP no h3.
- [ ] `go test ./...` passa no Windows, incluindo os testes do `transport` (config, TLS, e2e h2/h3 em loopback) e do `recorder`. Na VM, `make test` (`go test -race ./...` em contêiner) também passa.

## 6. Fase 2 — Ambiente de teste

### 6.1 Docker

- Um `Dockerfile` multi-stage (`golang:1.27.1-trixie` → `debian:trixie-slim` com `iproute2`). Os targets `server` e `loadgen` saem da mesma base. O target `analysis` parte de `python:3.13-slim-trixie` com o `requirements.txt`.
- A versão entra como build-arg (`VERSION`, gerada pelo `Makefile` com `git describe`), porque o `.git` não vai para a imagem.
- `compose.yaml`:
  - rede `benchnet` (bridge), com a porta 8443 em TCP e UDP;
  - `cap_add: [NET_ADMIN]` nos dois serviços;
  - `cpuset` separados via `SERVER_CPUS` e `LOADGEN_CPUS` (default para a VM de 6 vCPUs: servidor `2-3`, loadgen `4-5`; `0-1` ficam para o host);
  - volumes `./certs:ro` e `./results`;
  - variáveis vindas do `.env` do cenário;
  - healthcheck com `server healthcheck`, `start_interval` curto e `interval` longo; o loadgen espera `service_healthy`;
  - serviço `analysis` num profile próprio.
- O entrypoint de cada contêiner roda o `netem.sh apply` antes do binário.

### 6.2 Injeção de falhas (`netem.sh`)

- Variáveis: `NETEM_DELAY` (ms), `NETEM_JITTER` (ms), `NETEM_LOSS` (%), `NETEM_DIST` (normal).
- Aplicado no **egress do `eth0` dos dois contêineres**. Definição usada no TCC: *perda X% por direção*.
- Comandos: `apply`, `clear`, `outage <segundos>` (100% de perda temporária, depois restaura).

### 6.3 Cenários do POC

| Arquivo | Delay/lado | Jitter | Perda | Observação |
|---|---|---|---|---|
| `s1-baseline` | 0 | 0 | 0% | latência < 1 ms; mede o overhead do QUIC/TLS |
| `s2-loss-{1,3,5}` | 5 ms | 0 | 1/3/5% | capacidade sob perda (RTT base ~10 ms para a retransmissão pesar) |
| `s2-loss-{1,3,5}-low` | 5 ms | 0 | 1/3/5% | HoLB a `--rps=200`, abaixo da capacidade do h2 a 5% (~330 RPC/s) |
| `s3-jitter` | 20 ms | 10 ms (normal) + `rate 10gbit` | 0% | congestionamento/variação, sem reordenação |
| `s5-outage` | 5 ms | 0 | 0% + apagão de 5 s em t=30 s | tempo de recuperação |

> Os parâmetros de delay dos Cenários 2 e 3 são **valores iniciais** e serão calibrados no POC antes de serem fixados no texto do TCC.

**Carga (todos os cenários, exceto os `-low`):** `--rps=2000 --concurrency=500 --duration=60s --warmup=10s`.

- **Por que taxa fixa:** a primeira rodada do s1 em loop fechado (50 workers) saturou os 2 vCPUs do servidor e do loadgen. O h2 fez ~5.300 RPC/s com p50 de 8 ms; o h3 fez ~8.200 RPC/s com p50 de 5 ms. Nesse regime a latência é fila de CPU (Lei de Little), não transporte.
- **Por que 2000 RPC/s:** fica em ~40% da capacidade do h2, então a latência passa a refletir o transporte e as falhas injetadas. A vazão fica igual nos dois transportes por construção; comparam-se latência, erros e recuperação.
- **Por que 500 workers:** um worker livre precisa existir para cada RPC em voo, ou seja, taxa × latência (2000 × 250 ms = 500). O limite é ≤ 1000, o `MAX_CONCURRENT_STREAMS` do servidor h2 (§5.2).
- **Sinal de saturação:** `late_starts` > 0 fora do apagão indica loadgen ou workers insuficientes, e pede ajuste da taxa.

### 6.4 Runner (`scripts/run.sh`)

```
for scenario in experiments/scenarios/*.env
  for transport in h2 h3
    for rep in 1..REPS            # POC: REPS=5 (TCC final: ≥ 30)
      compose up -d server (com o .env do cenário)   → espera o health check
      inicia amostragem de docker stats (1 s)        → stats.csv
      [s5] agenda: docker exec server netem.sh outage 5 (em t=30 s) → events.csv
      compose run loadgen                             → requests.csv, meta.json
      coleta docker logs                              → server.log, loadgen.log
      compose down
```

- A ordem de h2/h3 é intercalada por repetição para diluir efeitos térmicos e de cache.
- No s5, o runner grava em `events.csv` os instantes reais (Unix ns) de início e fim do apagão. A análise precisa do fim do apagão, e o loadgen não sabe quando ele ocorre.
- Falha em uma repetição: registra em `failures.log` e segue. `set -euo pipefail`, com `trap` que sempre executa `compose down`.
- `run_id` = timestamp na hora local da VM, `YYYY-MM-DD-HH-MM-SS` (ex. `2026-09-28-11-11-52`). O commit fica em `meta.json.version`. Todo resultado é imutável: se a pasta já existir, o runner para.

### 6.5 Métricas e análise (`analysis/analyze.py`)

Entrada: a árvore `results/<run_id>/`. Só linhas com `phase=measure`. Roda no contêiner `analysis` (`make analyze`); o `pytest` roda no Windows e no contêiner.

| Métrica | Definição |
|---|---|
| Latência | p50, p90, p99, p99.9, média e desvio de `latency_us` (só `status=ok`) |
| Vazão | RPCs ok/s e MB/s na janela medida |
| Taxa de erro | RPCs com `status≠ok` / total |
| Tempo de recuperação (s5) | Do fim do apagão (lido do `events.csv`) até a vazão em janelas de 1 s voltar a ≥ 90% da média pré-apagão por 3 janelas seguidas |
| Recursos | CPU% e memória médios por contêiner (`stats.csv`) |

Saídas:

- `summary.csv`: uma linha por cenário × transporte, agregando as repetições (média e IC 95%).
- Gráficos: CDF de latência por cenário, boxplot de p99 por repetição, série temporal de vazão no `s5-outage`.
- Teste estatístico: Mann-Whitney U (h2 vs h3) por cenário, com o p-valor no `summary.csv`.
- `pytest` cobre o cálculo de percentis, vazão e tempo de recuperação com dados sintéticos.

### 6.6 Critérios de aceite da Fase 2

- [ ] `make run-all REPS=5` executa a matriz completa sem intervenção.
- [ ] Cada repetição tem `requests.csv`, `meta.json`, `stats.csv` e os logs; no s5, também o `events.csv`.
- [ ] `make analyze RUN=<id>` gera `summary.csv` e os gráficos.
- [ ] Sanidade: a latência p99 do h2 cresce de forma monotônica de 1% → 5% de perda.
- [ ] O `s5-outage` mostra a queda e a retomada da vazão, e o tempo de recuperação é calculado.
- [ ] Rodar o mesmo cenário duas vezes dá resultados dentro de uma variação aceitável (CV do p50 < 10%).

## 7. Caminho para o sistema do TCC

| Componente do POC | No sistema final |
|---|---|
| `internal/transport` | Usado por **todos** os serviços, como servidor e como cliente das chamadas entre serviços. Um único `TRANSPORT` global troca a pilha inteira. |
| `cmd/server` + `benchsvc` | Substituídos por `cmd/gateway`, `cmd/core`, `cmd/risk`, `cmd/ledger` + os seus `.proto`. |
| `cmd/loadgen` | Mesmo binário, passando a mirar o RPC do Gateway (ex.: `CreatePayment`). O `--method` vira uma flag. |
| `netem.sh` | Incluído no entrypoint de cada serviço. |
| `compose.yaml` | Ganha os serviços novos, o broker de mensageria e o banco. |
| `run.sh`, cenários, `analyze.py` | Sem mudança de contrato (mesmo CSV). O Cenário 4 entra como um novo `.env` + etapa no runner. |

## 8. Fora do escopo do POC

- Cenário 4 (migração de IP). O quic-go já expõe `AddPath`/`Probe`/`Switch`, então é viável depois.
- 0-RTT/resumption explícitos. No POC, o handshake é medido só na primeira conexão.
- RPCs de *streaming*, mensageria, banco de dados, Kubernetes/kind, Prometheus/Grafana.

## 9. Riscos e mitigação

| Risco | Mitigação |
|---|---|
| Buffers UDP pequenos degradam o quic-go (ele avisa nos logs) | `scripts/host-tuning.sh` (na VM): `net.core.rmem_max`/`wmem_max` = 7500000, registrado no `meta.json` |
| O quic-go consome mais CPU que o TCP do kernel | Registrar CPU por contêiner e discutir isso como resultado, não como defeito |
| O jitter do netem reordena pacotes | Confirmado na rodada de 2026-09-28: o s3 colapsa. O `netem.sh` usa `rate 10gbit` com jitter (FIFO); uma `pfifo` filha não evita a reordenação |
| Cliente e servidor no mesmo host competem por CPU | `cpuset` separados e checagem de saturação da CPU do loadgen |
| O loop fechado subestima a latência sob perda | Usar `--rps` nos cenários com perda para os números finais |
| O h2 abrir conexões extras sob carga | `StrictMaxConcurrentRequests: true` + `dials` no `meta.json` (repetição inválida se ≠ 1) |
| O quic-go é pré-1.0 e muda de API entre versões minor | Versões fixadas (seção 11) e congeladas durante as rodadas do TCC |

## 10. Sequência de execução sugerida

1. Scaffold do repo (`.gitattributes`, `.gitignore`, `go.mod`, `tools/go.mod`), `bench.proto`, `buf generate` no Windows (commit do `gen/`), `Makefile`.
2. `internal/transport` + testes (unitários + e2e h2/h3 em loopback).
3. `internal/loadgen` + `cmd/server` + `cmd/loadgen` (sem Docker) → smoke local como teste e2e em Go (`go test`), em h2 e h3.
4. `Dockerfile` + `compose.yaml` + `gen-certs.sh` → smoke em contêiner (**fim da Fase 1**).
5. `netem.sh` + cenários + `run.sh`.
6. `analyze.py` + testes.
7. Rodada completa com REPS=5, revisão dos resultados e calibração dos parâmetros (**fim da Fase 2**).

## 11. Versões fixadas

| Item | Versão |
|---|---|
| Go | 1.27.1 (`go.mod`: `go 1.27.0`, `toolchain go1.27.1`) |
| `github.com/quic-go/quic-go` | v0.63.0 |
| `connectrpc.com/connect` (+ `protoc-gen-connect-go`) | v1.21.0 |
| `google.golang.org/protobuf` (+ `protoc-gen-go`) | v1.36.12 |
| `github.com/bufbuild/buf` (só em `tools/go.mod`) | v1.73.0 |
| Testes em Go | só a stdlib `testing` |
| Imagens | `golang:1.27.1-trixie`, `debian:trixie-slim`, `python:3.13-slim-trixie` |
| Python | 3.13 · pandas 3.0.6 · numpy 2.5.3 · matplotlib 3.11.2 · scipy 1.18.1 · pytest 9.1.1 (fixadas com `==`) |

## Referências técnicas

- McDonald, K. *gRPC over HTTP/3* e *Followup* — https://kmcd.dev/posts/grpc-over-http3-followup/
- quic-go — Connection Migration — https://quic-go.net/docs/quic/connection-migration/
- ConnectRPC — https://connectrpc.com/
- `x/net/http2` — README sobre a mudança para a stdlib no Go 1.27 — https://github.com/golang/net/blob/v0.59.0/http2/README.md
- wrk2 — latência corrigida para *coordinated omission* — https://github.com/giltene/wrk2
