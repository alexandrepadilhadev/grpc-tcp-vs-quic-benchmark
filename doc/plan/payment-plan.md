# Plano do sistema de pagamentos — gRPC sobre HTTP/2 (TCP) vs HTTP/3 (QUIC)

> Status: **rev. 2 — aprovada; Fases P1–P5 concluídas** · Última revisão: 2026-10-17
> Rev. 2: taxas e regras da calibração (§9: C = saturação, taxa alta 300, baixa 200, corte do s2 de capacidade, `make fitness`, `late_starts` só reportado), alias `ledger-rpc` (§8.1), filas duráveis (§2, §6.3), `timeline.py` (§11), campo `http` no `rpclog` (§11); Fases P1–P5 concluídas, resultados em [`../PAYMENT-RUN-ANALYSIS.md`](../PAYMENT-RUN-ANALYSIS.md).
> Contexto: TCC "Desempenho do gRPC sobre TCP e QUIC" (Alexandre Padilha, UTFPR), §3.3 e Figura 5
> Antecedente: um POC de um salto (tag `poc-v1`, `59e95eb`) validou o transporte, a carga e a análise; o material dele está em [`../archive/poc/`](../archive/poc/) e não é mais usado.
> Arquitetura atual: [`../architecture.md`](../architecture.md).

## 1. Objetivo

Implementar o gateway de pagamentos do TCC (Gateway, Core, Risk e Ledger, com mensageria e banco de dados) e medir nele o efeito do transporte.

**Pergunta:** qual é o efeito do transporte (TCP+HTTP/2 × QUIC+HTTP/3) sobre a latência, a vazão, os erros e a recuperação ponta a ponta de um sistema de pagamentos com 3 saltos gRPC síncronos, em cada condição de rede: rede ideal, perda, jitter e apagão.

**Requisito estrutural:** o único fator variável é o transporte dos saltos gRPC síncronos. Todo o resto é igual entre h2 e h3 e, dentro do possível, sai do caminho medido.

Fora desta fase (próximas fases, [`../PAYMENT-RUN-ANALYSIS.md`](../PAYMENT-RUN-ANALYSIS.md) §8):
- **Cenário 4** (migração de IP): fase própria, com spec própria (§13).
- **Consistência sob timeout** (pagamentos liquidados que o cliente deu como falhos) e **onde está a falha** (salto de borda × salto interno).

## 2. Decisões tomadas

Herdadas do POC, sem mudança: Go + connect-go com `connect.WithGRPC()`; h2 via `net/http`, h3 via `quic-go/http3`; TLS 1.3 nos dois; Docker Compose + Makefile + bash; netem dentro dos contêineres; análise em Python; sem Kubernetes, Prometheus/Grafana ou 0-RTT explícito.

| Tema | Decisão | Motivo |
|---|---|---|
| Transporte | `internal/transport` **sem mudança de API**. Um único `TRANSPORT` troca todos os saltos gRPC síncronos; uma conexão por par cliente→servidor (`dials = 1` em cada salto) | O transporte é o único fator variável |
| Adquirente | **Stub dentro do Core:** aprova tudo, devolve um código de autorização e aplica um atraso fixo configurável (`ACQUIRER_DELAY`, padrão 0) | Para a pergunta da fase, um salto a mais não acrescenta nada medível. Os "pagamentos não processados por timeout" do TCC aparecem como `deadline_exceeded` nos saltos gRPC |
| Quem chama o Ledger | **O Core** (Gateway → Core → Ledger, em cadeia) | Bate com a Figura 5 e com o texto do TCC ("após as validações, o fluxo prossegue"). A outra opção tem o mesmo número de travessias de rede |
| Resultado do Risk | **Totalmente assíncrono:** o Core publica o evento fora do caminho crítico; o Risk publica o resultado; o Core só o consome e conta | Um caminho que espera o broker entraria na latência sem variar com o transporte e diluiria o efeito por construção |
| Broker | **RabbitMQ 4** (AMQP 0-9-1, cliente `amqp091-go`), um nó, filas duráveis (o RabbitMQ 4 descontinua filas transitórias não exclusivas) e mensagens transitórias | É o AMQP que os docs citam. Nada de disco no caminho |
| Banco | **PostgreSQL 18** com `pgx/v5` e pool, `synchronous_commit=off`, sem volume (base nova a cada repetição) | ACID com lock de linha. Sem esperar fsync, o disco da VM sai do caminho medido; a durabilidade não está sob teste |
| Concorrência de saldo | **UPDATE condicional atômico** numa transação curta, com o `payment_id` como chave de idempotência; 10.000 contas com saldo "infinito"; conta sorteada de forma uniforme | 1 lock de linha por comando e contenção desprezível. O banco não vira variável ("lógica mínima") |
| Lógica de negócio | **Mínima e determinística:** valor fixo, aprova tudo, saldo nunca acaba | Menos variância que não vem do transporte |
| Netem | **Só nos saltos gRPC** (rede `rpcnet`); banco e broker numa rede limpa (`datanet`) | "A rede entre os serviços está ruim" replica o POC salto a salto. O TCP do Postgres e do RabbitMQ sofreria igual nos dois transportes, o que seria ruído |
| CPUs | **Um núcleo dedicado por salto crítico:** host `0`, loadgen `1`, gateway `2`, core `3`, ledger `4`; postgres, rabbitmq e risk dividem o `5` | Isolamento como no POC. Num pool compartilhado, o escalonador misturaria os serviços, e esse ruído é o mais difícil de detectar ([`../archive/poc/todo.md`](../archive/poc/todo.md) itens 12–13) |
| Taxas de carga | **Calibradas numa fase própria** (§9). A taxa alta fica em ~40% da capacidade medida com h3 no s1; a taxa baixa começa em 200 RPC/s e é confirmada com h2 a 5% de perda | Com 4 serviços em 6 vCPUs, 2000 RPC/s não cabe. Regra herdada do POC |
| Mensagens | **Tamanho natural do domínio** (~50–100 B), sem preenchimento; `--payload-bytes` e `--response-bytes` saem do loadgen | Lógica mínima e realista. A comparação é entre h2 e h3 no mesmo sistema, não com os números absolutos do POC |
| Contrato `.proto` | **Um pacote `payment.v1`** com os 3 serviços gRPC e as 2 mensagens de evento | Os tipos são compartilhados, e um pacote por serviço (como dizia o `architecture.md` §8.2) só duplicaria código. Correção do doc em §12, Fase P5 |
| POC | `cmd/server`, `internal/benchsvc` e `proto/bench` são **removidos**; o POC continua reproduzível pela tag **`poc-v1`** (`59e95eb`) | Uma única camada de serviço no repositório |
| Observabilidade | **Rodada de trace** separada das medições: `slog` em `debug`, `payment_id` em todo log, linha do tempo juntada por script (§11). Sem OpenTelemetry/Jaeger | Reaproveita o `rpclog` e o `slog`; nada de coletor nem SDK disputando CPU |

## 3. Arquitetura

```
 rpcnet 10.10.0.0/24 (netem no egress desta interface, em todos os contêineres dela)
 ┌─────────┐ gRPC h2|h3 ┌─────────┐ gRPC h2|h3 ┌─────────┐ gRPC h2|h3 ┌─────────┐
 │ loadgen │──────────► │ gateway │──────────► │  core   │──────────► │ ledger  │
 │ CPU 1   │ :8443      │ CPU 2   │ :8443      │ CPU 3   │ :8443      │ CPU 4   │
 └─────────┘            └─────────┘            └────┬────┘            └────┬────┘
 ...........................................................................│.....
 datanet 10.20.0.0/24 (limpa)                       │ AMQP                 │ SQL
                         ┌──────────────────────────┼──────────┐           │
                         │ CPU 5                    ▼          │           ▼
                         │  ┌──────┐ payment.events ┌────────┐ │     ┌──────────┐
                         │  │ risk │◄───────────────│rabbitmq│ │     │ postgres │
                         │  │      │───────────────►│        │ │     │  CPU 5   │
                         │  └──────┘  risk.results  └───┬────┘ │     └──────────┘
                         └──────────────────────────────┼──────┘
                                                        └──► core (consome risk.results)
```

- **Saltos medidos:** loadgen→gateway, gateway→core e core→ledger. Cada um é **uma** conexão `transport.Client` (TLS 1.3, `x-served-proto` conferido), e todos trocam juntos com o `TRANSPORT`.
- **Fora da comparação:** AMQP (core⇄rabbitmq⇄risk) e SQL (ledger⇄postgres) usam TCP nos dois transportes, na `datanet`, sem netem.
- **Fan-out = 1 em todo salto (invariante):** cada RPC de entrada gera exatamente uma RPC em cada salto interno. Assim, as RPCs em voo em qualquer conexão interna nunca passam das em voo na borda (`--concurrency` = 500), abaixo do limite de 1000 streams que trava o cliente h2 do Go com `StrictMaxConcurrentRequests` (`architecture.md` §3.2).
- **Prazo:** só o loadgen define o prazo (2 s). O connect-go o propaga em cada salto pelo header `grpc-timeout`, e o Ledger o repassa às consultas `pgx`. **Ninguém tenta de novo:** um erro em qualquer salto volta ao cliente com o código gRPC original.

## 4. Estrutura do repositório

Só o que mudou em relação ao POC ([`../archive/poc/plan/poc-plan.md`](../archive/poc/plan/poc-plan.md) §4):

```
.
├── proto/payment/v1/payment.proto   # novo; substitui proto/bench
├── gen/payment/v1/…                 # gerado pelo buf (versionado)
├── cmd/
│   ├── gateway/main.go              # novo
│   ├── core/main.go                 # novo
│   ├── ledger/main.go               # novo
│   ├── risk/main.go                 # novo
│   └── loadgen/main.go              # passa a chamar PaymentService.CreatePayment
├── internal/
│   ├── transport/                   # sem mudança
│   ├── rpclog/                      # + campo side=client|server; registrado também nos clientes
│   ├── service/                     # novo: config comum, log, /healthz, healthcheck, svc-<nome>.json
│   ├── gateway/                     # handler do PaymentService
│   ├── core/                        # handler do CoreService, stub do adquirente, publicador e consumidor AMQP
│   ├── ledger/                      # handler do LedgerService, Store (pgx) + fake em memória para testes
│   ├── risk/                        # consumidor AMQP, pontuação, publicador
│   └── loadgen/                     # sem mudança de contrato
├── deploy/
│   ├── Dockerfile                   # target "services" (4 binários) no lugar de "server"
│   ├── compose.yaml                 # 4 serviços + rabbitmq + postgres, 2 redes, cpusets
│   ├── netem.sh                     # escolhe a interface pela sub-rede (NETEM_SUBNET)
│   └── postgres/init.sql            # novo: schema + 10.000 contas
├── scripts/
│   ├── gen-certs.sh                 # SAN: gateway, core, ledger, localhost
│   └── run.sh                       # gateway no lugar de server, REP_DIR, OUTAGE_SVC, LOADGEN_EXTRA, TRACE
├── experiments/scenarios/*.env      # mesmos cenários; taxas atualizadas na Fase P4
└── analysis/
    ├── analyze.py                   # dials por salto, rpc_cpu
    └── timeline.py                     # novo: linha do tempo da rodada de trace
```

Saem: `cmd/server`, `internal/benchsvc`, `proto/bench`, `gen/bench`.

## 5. Contratos (`proto/payment/v1/payment.proto`)

```proto
syntax = "proto3";
package payment.v1;

// Entry point, called by the loadgen.
service PaymentService {
  rpc CreatePayment(CreatePaymentRequest) returns (CreatePaymentResponse);
}

// Business rules + acquirer authorization (stub) + settlement via Ledger.
service CoreService {
  rpc ProcessPayment(ProcessPaymentRequest) returns (ProcessPaymentResponse);
}

// Balance update and final state.
service LedgerService {
  rpc Settle(SettleRequest) returns (SettleResponse);
}

enum PaymentStatus {
  PAYMENT_STATUS_UNSPECIFIED = 0;
  PAYMENT_STATUS_SETTLED     = 1;
  PAYMENT_STATUS_DECLINED    = 2; // insufficient funds or unknown account
}

message CreatePaymentRequest {
  string payment_id   = 1; // generated by the client; correlation + idempotency key
  uint32 account_id   = 2;
  int64  amount_cents = 3;
}
message CreatePaymentResponse {
  string        payment_id = 1;
  PaymentStatus status     = 2;
  string        auth_code  = 3;
}

message ProcessPaymentRequest {
  string payment_id   = 1;
  uint32 account_id   = 2;
  int64  amount_cents = 3;
}
message ProcessPaymentResponse {
  PaymentStatus status    = 1;
  string        auth_code = 2;
}

message SettleRequest {
  string payment_id   = 1;
  uint32 account_id   = 2;
  int64  amount_cents = 3;
  string auth_code    = 4;
}
message SettleResponse {
  PaymentStatus status = 1;
}

// AMQP bodies (protobuf binary), queues payment.events and risk.results.
message PaymentEvent {
  string payment_id   = 1;
  uint32 account_id   = 2;
  int64  amount_cents = 3;
  int64  created_ns   = 4; // Unix ns, set by the Core
}
message RiskResult {
  string payment_id = 1;
  uint32 score      = 2; // 0..99
}
```

- Mensagens separadas por RPC (convenção do buf), mesmo com campos repetidos.
- Erros: `InvalidArgument` (campo inválido), `DeadlineExceeded`/`Canceled` (prazo), `Unavailable` (salto fora do ar) e `Internal` (erro do banco). `DECLINED` não é erro: volta como status.

## 6. Serviços e fluxos

### 6.1 Molde comum (`internal/service`)

Cada `cmd/<serviço>` segue o molde do `cmd/server` do POC, fatorado num pacote porque os 4 binários o repetem:
- **Configuração:** `transport.Config` com `RoleServer|RoleClient` (servidor + clientes + `healthcheck`), `--log-level`/`LOG_LEVEL` e as flags do serviço (§6.3). Precedência: flag > env > padrão; erro de configuração sai com código ≠ 0 antes de abrir qualquer socket.
- **Servidor:** `transport.Listen` + `Serve` com o handler connect (gzip não registrado, `rpclog.Interceptor`) e `GET /healthz`; o subcomando `healthcheck` é idêntico ao do POC.
- **Clientes:** um `transport.NewClient` por salto de saída, com `connect.WithGRPC()`, sem gzip e com `rpclog.Interceptor` (`side=client`). Um interceptor confere o `x-served-proto` de toda resposta (`VerifyServedProto`; custa uma leitura de header); divergência falha a RPC com `Internal`.
- **Encerramento:** no SIGTERM, shutdown graceful e, se `REP_DIR` estiver definido, grava `REP_DIR/svc-<nome>.json`:

  ```json
  {"service":"core","version":"…","transport":"h3","dials":{"ledger-rpc:8443":1},
   "counters":{"events_published":71234,"events_dropped":0,"risk_results":71230}}
  ```

### 6.2 Fluxo síncrono

```
loadgen ── CreatePayment ──► gateway ── ProcessPayment ──► core ── Settle ──► ledger ── SQL ──► postgres
        ◄─ status, auth ───          ◄─ status, auth ────       ◄─ status ──          ◄─ commit ─
```

1. **Gateway:** valida `payment_id` (não vazio, ≤ 64 B), `account_id > 0` e `amount_cents > 0`, senão `InvalidArgument`. Chama `CoreService.ProcessPayment` com o `ctx` recebido e devolve o resultado com o `payment_id`.
2. **Core:**
   1. valida de novo (cada serviço protege o próprio contrato);
   2. **adquirente (stub):** espera `ACQUIRER_DELAY` e gera `auth_code` = 6 caracteres derivados do `payment_id`;
   3. **evento:** põe um `PaymentEvent` numa fila em memória de 10.000 posições, sem bloquear; se a fila estiver cheia, descarta e incrementa `events_dropped`;
   4. chama `LedgerService.Settle` e devolve `status` e `auth_code`.
3. **Ledger:** uma transação `pgx`:

   ```sql
   BEGIN;
   INSERT INTO entries (payment_id, account_id, amount_cents, auth_code)
     VALUES ($1, $2, $3, $4) ON CONFLICT (payment_id) DO NOTHING;   -- 0 rows → já liquidado: COMMIT, SETTLED
   UPDATE accounts SET balance_cents = balance_cents - $3
     WHERE id = $2 AND balance_cents >= $3;                         -- 0 rows → ROLLBACK, DECLINED
   COMMIT;                                                          -- SETTLED
   ```

   Erro do banco vira `Internal`; prazo vencido vira `DeadlineExceeded`. Pool `pgxpool` com `DB_MAX_CONNS` (padrão 8).

**Schema** (`deploy/postgres/init.sql`, executado na criação do contêiner):

```sql
CREATE TABLE accounts (id integer PRIMARY KEY, balance_cents bigint NOT NULL);
CREATE TABLE entries (
  payment_id   text PRIMARY KEY,
  account_id   integer NOT NULL,
  amount_cents bigint NOT NULL,
  auth_code    text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);
INSERT INTO accounts SELECT g, 1000000000000000 FROM generate_series(1, 10000) g;
```

### 6.3 Fluxo assíncrono

```
core (goroutine publicadora) ── PaymentEvent ──► [payment.events] ──► risk
core (consumidor)            ◄── RiskResult  ─── [risk.results]   ◄── risk
```

- **Topologia:** duas filas na exchange padrão, `payment.events` e `risk.results`, declaradas pelos dois lados de forma idempotente (duráveis, sem auto-delete; o contêiner é recriado a cada repetição). Mensagens transitórias, corpo em protobuf binário.
- **Core, publicador:** uma goroutine drena a fila em memória e chama `PublishWithContext`, sem *publisher confirms*. Conta `events_published`.
- **Risk:** consome `payment.events` (auto-ack, prefetch 100), calcula `score = FNV-1a(payment_id) mod 100`, publica `RiskResult` em `risk.results` e conta `events_consumed` e `results_published`.
- **Core, consumidor:** consome `risk.results` (auto-ack) e conta `risk_results`. Não muda o estado do pagamento.
- **Conexões:** uma conexão AMQP por serviço, com um canal para publicar e outro para consumir. Se a conexão cair, o serviço loga em `error` e sai com código ≠ 0. O Compose não reinicia o serviço, e a repetição falha de forma visível.
- **Flags:**
  - gateway: `--core-addr`/`CORE_ADDR` (padrão `core:8443`);
  - core: `--ledger-addr`/`LEDGER_ADDR` (`ledger:8443`), `--amqp-url`/`AMQP_URL`, `--acquirer-delay`/`ACQUIRER_DELAY` (`0s`);
  - ledger: `--db-url`/`DATABASE_URL`, `--db-max-conns`/`DB_MAX_CONNS` (`8`);
  - risk: `--amqp-url`/`AMQP_URL`.

### 6.4 Loadgen

- Chama `PaymentService.CreatePayment` em `--target` (padrão passa a ser `gateway:8443`). O resto (agenda, prazo, probe, recorder, `meta.json`) não muda.
- Cada RPC leva um `payment_id` novo (16 bytes aleatórios em hex), `account_id` uniforme em `1..--accounts` (padrão 10000) e `amount_cents = 1000`.
- `req_bytes` = `proto.Size` de um pedido típico (fixo por execução) e `resp_bytes` = `proto.Size` da resposta. O contrato do `requests.csv` não muda.
- `status=ok` só quando o RPC dá certo **e** o status é `SETTLED`. `DECLINED` não deve ocorrer, porque o saldo é "infinito"; se ocorrer, a chamada devolve `connect.CodeFailedPrecondition` e entra como `failed_precondition` no CSV, sem mudar o runner, para nada sumir das estatísticas.
- **O probe atravessa a cadeia toda:** abre as três conexões em série, então o `handshake_us` passa a ser o tempo de abrir os três saltos mais uma RPC (§10).
- Ganha `--log-level`/`LOG_LEVEL`. Em `debug`, loga uma linha antes e uma depois de cada RPC (§11).

## 7. Deploy

### 7.1 Imagens

- `Dockerfile`: o target `server` vira `services`, com os 4 binários e `ENTRYPOINT ["/usr/local/bin/netem.sh", "run"]`; o Compose passa o binário em `command`. Os targets `loadgen` e `analysis` não mudam.
- `postgres:18-trixie` e `rabbitmq:4` (sem o plugin de gestão, que coleta estatísticas e gastaria CPU no núcleo 5 dividido), com versão fixada na Fase P2 (§15).

### 7.2 `compose.yaml`

| Serviço | Redes | cpuset | Healthcheck | Depende de (healthy) |
|---|---|---|---|---|
| loadgen (profile `load`) | rpcnet | `LOADGEN_CPUS` = `1` | — | gateway |
| gateway | rpcnet | `GATEWAY_CPUS` = `2` | `gateway healthcheck` | core |
| core | rpcnet, datanet | `CORE_CPUS` = `3` | `core healthcheck` | ledger, rabbitmq |
| ledger | rpcnet, datanet | `LEDGER_CPUS` = `4` | `ledger healthcheck` | postgres |
| risk | datanet | `DATA_CPUS` = `5` | — | rabbitmq |
| rabbitmq | datanet | `DATA_CPUS` = `5` | `rabbitmq-diagnostics -q check_port_connectivity` (intervalo de 1 h depois de saudável) | — |
| postgres | datanet | `DATA_CPUS` = `5` | `pg_isready` | — |

- **Redes com sub-redes fixas:** `rpcnet` 10.10.0.0/24 e `datanet` 10.20.0.0/24.
- **Serviços Go:** `cap_add: [NET_ADMIN]`, volumes `../certs:/certs:ro` e `../results:/results`, variáveis `<<: *netem` mais `NETEM_SUBNET=10.10.0.0/24`, `TRANSPORT`, `LOG_LEVEL`, `REP_DIR` e `QLOGDIR`.
- **RabbitMQ:** `RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="+S 1:1 +sbwt none +sbwtdcpu none +sbwtdio none"`. O Erlang fica com um escalonador e sem *busy-wait*, que queimaria o núcleo 5 dividido.
- **Postgres:** `command: postgres -c synchronous_commit=off -c max_connections=50 -c shared_buffers=256MB`; `init.sql` montado em `/docker-entrypoint-initdb.d/`.
- **Memória** (não limitada; esperado): postgres e rabbitmq < 512 MB cada, serviços Go < 150 MB cada; total < 2 GB dos 8 GB.
- **Certificado:** o `gen-certs.sh` passa a emitir `subjectAltName = DNS:gateway,DNS:core,DNS:ledger,DNS:localhost,IP:127.0.0.1`. Um único certificado para os três servidores, como no POC. Certificados antigos precisam ser apagados (`rm -rf certs && make certs`).

## 8. Injeção de falhas e cenários

### 8.1 `netem.sh`

- **Mudança única:** se `NETEM_SUBNET` estiver definido, a interface é a que tem endereço nessa sub-rede (`ip -o -4 addr show to "$NETEM_SUBNET"`). Se nenhuma tiver, o script falha com erro. Sem a variável, continua `NETEM_IFACE` (padrão `eth0`), como no POC.
- **Efeito:** loadgen, gateway, core e ledger aplicam o netem só na interface da `rpcnet`. Cada travessia de cada salto gRPC sofre o atraso e a perda **uma vez por direção**, a mesma definição do TCC e do POC.
- **O salto core→ledger usa o alias `ledger-rpc`**, que só existe na `rpcnet` (`LEDGER_ADDR=ledger-rpc:8443`, com o nome no certificado). Core e ledger estão nas duas redes, e o nome `ledger` pode resolver para o IP da `datanet`, onde o netem não atua.
- A `datanet` nunca tem netem. O risk, o rabbitmq e o postgres não usam o `netem.sh`.

### 8.2 Cenários

Mesmos arquivos e mesmos parâmetros de rede do POC. Só o `LOADGEN_ARGS` muda na Fase P4.

| Arquivo | Atraso/lado | Jitter | Perda | Carga | Leitura |
|---|---|---|---|---|---|
| `s1-baseline` | 0 | 0 | 0% | alta | custo do QUIC/TLS em 3 saltos |
| ~~`s2-loss-{1,3,5}`~~ | 5 ms | 0 | 1/3/5% | alta | capacidade sob perda: **saiu da matriz** pela regra de corte da §9 (taxa alta 300 ≤ 2 × 200); a capacidade sob perda fica com o POC |
| `s2-loss-{1,3,5}-low` | 5 ms | 0 | 1/3/5% | baixa | latência e HoLB, sem fila |
| `s3-jitter` | 20 ms | 10 ms (normal) + `rate 10gbit` | 0% | alta | variação de atraso |
| `s5-outage` | 5 ms | 0 | 0% + apagão de 5 s no **gateway** | alta | recuperação na mesma conexão |

- **Base ponta a ponta:** com 3 saltos, o RTT base passa a ser 30 ms (s2 e s5) e 120 ms (s3).
- **Perda acumulada:** uma RPC atravessa 6 trechos. Supondo um pacote por trecho (mensagens de ~100 B), a chance de ela passar sem nenhuma perda é ~94%, ~83% e ~74% a 1%, 3% e 5%.
- **Apagão do s5:** 100% de perda no egress da `rpcnet` do gateway (`OUTAGE_SVC=gateway`, padrão do `run.sh`). Ele corta as respostas ao loadgen e os pedidos ao core, como o apagão do "servidor" no POC. O `events.csv` não muda.

## 9. Carga e calibração

- **Fixo, como no POC:** `--concurrency=500`, `--duration=60s`, `--warmup=10s`, `--deadline=2s`.
- **Etapa de calibração (Fase P4):** `make calibrate RATES="250 500 750 1000 1500"` roda o `s1-baseline` com `REPS=3` por taxa, nos dois transportes, em `results/calib-<varredura>-<taxa>-<horário>/`; o relatório usa só a varredura que acabou de rodar.
  - Por baixo, é o `run.sh` com `LOADGEN_EXTRA=--rps=<taxa>`, acrescentado depois do `LOADGEN_ARGS`; no `flag` do Go, o último valor vence.
- **Capacidade (C):** a capacidade de saturação, como no POC: a maior taxa da varredura em que o **h3** (pior caso de CPU) tem `served_frac ≥ 0,99` em todas as repetições. Com a granularidade da varredura, C é um limite inferior.
- **Taxa alta:** ~0,4 × C, arredondada para baixo em múltiplos de 50. É a mesma regra do POC, onde 2000 era ~40% da capacidade de saturação do h2 (~5300 RPC/s).
  - **Escolhida: 300 RPC/s**, um degrau abaixo de 0,4 × C = 400. A 400 RPC/s, o p50 do h3 ficaria em ~1,45× já num ambiente limpo, no limite da checagem de saúde, e o custo de CPU da VM variou 1,3–2× entre dias (conferência de 2026-10-14: p50 1,9–2,5×). A 300, a interpolação linear da varredura limpa (entre 250 e 500) dá, no h3, p50 ~3,7 ms (~1,15×), núcleo 5 em ~34% e core em ~28%. Detalhes em `doc/PAYMENT-RUN-ANALYSIS.md` §Calibração.
- **Checagem de saúde da taxa alta** (coluna `healthy` da tabela; a conferência da P4 roda o s1 com h3 nela). Em todas as repetições:
  - p50 até 1,5× o p50 da menor taxa;
  - CPU média ≤ 70% do cpuset em **todo** contêiner (gateway, core, ledger, loadgen e os três do núcleo 5 somados).
  - Na rev. 1, essas regras definiam o próprio C, e o 0,4 × C contava a folga duas vezes: a varredura de 2026-10-13 deu C = 250 e taxa alta = 100, abaixo da taxa baixa. Por isso C voltou a ser a saturação.
- **`late_starts`:** reportado na tabela, mas não decide nada. Nos diagnósticos de 2026-10-12, a 250 RPC/s, ele ficou em 4–15% dos slots com todos os contêineres abaixo de 25% de CPU, e não caiu com o loadgen em 2 vCPUs. A causa é a VM demorar mais de 1 ms para acordar o goroutine que dispara os pedidos, que dorme entre slots em taxas baixas; não é saturação. Como a latência é medida desde o horário planejado, esse atraso já entra nela, igual nos dois transportes.
- **Taxa baixa:** 200 RPC/s, confirmada se o **h2** no `s2-loss-5-low` tiver `served_frac ≥ 0,99` e erros ≤ 1% em 3 repetições. Se não passar, cai em degraus de 50 até passar, e a mesma taxa vale para os três `-low`.
  - **Confirmada: 200 RPC/s, prazo de 2 s** (conferência de 2026-10-14: h2 com 100% de demanda atendida e 0 erros nas 3 repetições).
- **Regra de corte do s2 de capacidade:** se a taxa alta for ≤ 2× a baixa, os `s2-loss-{1,3,5}` saem da matriz. **Aplicada** (300 ≤ 400): com 3 saltos, cada um com sua conexão, o h2 a 300 RPC/s só saturaria a 5% de perda, e a taxas mais altas o gargalo passa a ser a CPU, que se misturaria com o efeito da perda. A capacidade sob perda fica respondida pelo POC.
- **Aptidão do ambiente (`make fitness`):** antes do `make run-all` e do `make calibrate`, uma repetição de s1 com h2 a 250 RPC/s por 30 s. Se o `cpu_per_krps` passar de 200 (`analysis/fitness.py`), a rodada não começa. Referência limpa, com a vCPU reservada no Hyper-V: 134–172; host degradado: 268–600. O ruído de host afeta a rodada inteira por igual, e o `cpu_outlier`, que compara as repetições entre si, não o vê. `FITNESS=0` pula a checagem.
- **Prazo:** se no `s2-loss-5-low` o h2 tiver erros > 1% por `deadline_exceeded`, o `--deadline` sobe para 5 s em **todos** os cenários e nos dois transportes. A decisão e o motivo vão para a seção de calibração do doc de resultados.

## 10. Métricas e análise

O contrato de entrada não muda (`requests.csv`, `meta.json`, `stats.csv`, `events.csv`); ganha um arquivo novo, opcional. Mudanças no `analyze.py`:

| Item | Mudança |
|---|---|
| Validade da repetição | Além do `dials = 1` do `meta.json`, todo `svc-*.json` da repetição precisa ter todos os `dials` = 1. Sem `svc-*.json` (rodadas do POC), vale só a regra antiga |
| `rpc_cpu` (nova) | soma de `gateway_cpu + core_cpu + ledger_cpu`. As colunas `<contêiner>_cpu`/`_mem_mib` já saem por contêiner (`bench-<nome>-1`) |
| `cpu_per_krps` | passa a ser `rpc_cpu ÷ (ok_rps/1000)` quando existir `rpc_cpu`; senão, `server_cpu`, como no POC |
| `handshake_ms` | mesma coluna; agora é o tempo de abrir os 3 saltos em série mais uma RPC (teoria: h2 ≈ 3 × 3 RTT de salto, h3 ≈ 3 × 2) |
| Resto | sem mudança: percentis, `p*_all`, `served_frac`, `recovery_s`, `degraded_s`, mediana, HL, Cliff, Holm, razão |

O `run.sh` não muda o glob: `results/<run>/<cenário>/<transporte>/rep-<n>/`. Cada repetição passa a ter:
- `gateway.log`, `core.log`, `ledger.log`, `risk.log`, `rabbitmq.log` e `postgres.log`, no lugar do `server.log`;
- `svc-gateway.json`, `svc-core.json`, `svc-ledger.json` e `svc-risk.json`.

Mudanças no `run.sh`:
- espera `up -d --wait gateway risk`;
- exporta `REP_DIR=/results/<rep>`;
- usa `--target=gateway:8443`;
- `OUTAGE_SVC` (padrão `gateway`);
- `LOADGEN_EXTRA`;
- espera `DRAIN_SECS` (padrão 2) depois do loadgen, antes do `down`, para o caminho assíncrono esvaziar;
- com `QLOG=1`, grava o `nstat` do gateway.

## 11. Rodada de trace

**Objetivo:** explicar cada passo do sistema para um pagamento, do cliente ao Risk, sem afetar as medições.

- **Correlação:** todo log de domínio e todo `rpclog` levam o `payment_id`. O `rpclog` o tira do pedido quando a mensagem tem esse campo (interface `GetPaymentId() string`); o Risk o tira do evento.
- **Três camadas, todas em `debug`** (desligadas em `info`, custo de uma checagem de nível):

| Camada | Mensagens (`msg`) e campos |
|---|---|
| Cliente (loadgen) | `payment request` (`account_id`, `amount_cents`) · `payment response` (`latency_ms`, `code`, `status`, `auth_code`) |
| Por RPC (`rpclog`) | `rpc` com `side`, `procedure`, `code`, `duration_us`, `peer`, `protocol` (protocolo RPC: `grpc`) e, nas linhas de cliente, `http` (versão servida, do `x-served-proto`) |
| Domínio | gateway: `payment received` · core: `payment received`, `acquirer approved` (`auth_code`), `event queued`, `event published`, `risk result received` (`score`) · ledger: `settle received`, `debit applied` (`balance_cents`) ou `declined` · risk: `event received`, `risk scored` (`score`), `result published` |

- **Execução:** `make trace TRANSPORT=h2|h3 [SCENARIO=s1-baseline]`:
  - `LOG_LEVEL=debug` em todos os serviços e no loadgen;
  - `--requests=20 --rps=5 --warmup=0`;
  - saída em `results/trace-<timestamp>/<transporte>/`, fora do glob da análise.
- **Linha do tempo:** `analysis/timeline.py <dir>` junta os `*.log` JSON pelo campo `time` (o mesmo relógio do kernel da VM em todos os contêineres) e grava `timeline.txt`, agrupado por `payment_id`, com o tempo relativo à primeira linha do pagamento:

  ```text
  payment 7f3a9c… (HTTP/3.0)
    +0.000 ms  loadgen  payment request account_id=4821 amount_cents=1000
    +0.412 ms  gateway  payment received
    …
    +3.105 ms  loadgen  payment response latency_ms=3.1 code=ok status=SETTLED
    +4.870 ms  core     risk result received score=12
  ```

## 12. Fases e critérios de aceite

Uma fase por vez; a próxima só começa depois de os critérios serem confirmados na VM. No Windows: `go build ./...`, `go vet ./...`, `go test ./...` e `pytest analysis`.

### Fase P1 — Contratos, serviços gRPC e loadgen (sem infraestrutura)

- Tag `poc-v1` em `59e95eb`.
- `payment.proto` + `buf generate`; remoção de `proto/bench`, `gen/bench`, `cmd/server` e `internal/benchsvc`.
- `internal/service`; pacotes `internal/{gateway,core,ledger}` com `Store` fake em memória e fila de eventos sem broker; `rpclog` com `side` e `payment_id`; loadgen no `CreatePayment`. Os `cmd/<serviço>` entram na P2, junto com o banco e o broker.

**Aceite:**
- [ ] `go build ./... && go vet ./... && go test ./...` passam no Windows.
- [ ] Teste e2e em Go, em loopback, para h2 e h3: loadgen-runner → gateway → core → ledger(fake), com 1000 RPCs `ok`, `dials = 1` em cada um dos 3 clientes e `x-served-proto` correto em cada salto.
- [ ] Teste de prazo: um ledger fake que demora mais que o prazo faz o loadgen receber `deadline_exceeded`, e o handler do ledger vê o `ctx` cancelado (prova a propagação).
- [ ] Teste do Ledger contra o fake: `payment_id` repetido não debita duas vezes; saldo insuficiente → `DECLINED`.
- [ ] Na VM: `make test` (`go test -race ./...` em contêiner) passa.

### Fase P2 — Infraestrutura e smoke

- Ledger com `pgx`; core e risk com `amqp091-go`; `Dockerfile` (target `services`), `compose.yaml`, `init.sql`, `netem.sh` com `NETEM_SUBNET`, SAN no `gen-certs.sh`, `make smoke`.

**Aceite (na VM):**
- [ ] `rm -rf certs && make certs build` sem erro.
- [ ] `make smoke TRANSPORT=h2` e `TRANSPORT=h3`: 1000 RPCs `ok` com `--strict`; `meta.json` com `dials = 1`; os três `svc-*.json` com todos os `dials = 1`.
- [ ] Caminho assíncrono, depois do dreno: `events_published` (core) = `events_consumed` (risk) = `results_published` (risk) = `risk_results` (core) = 1001 (1000 + probe), e `events_dropped = 0`.
- [ ] `docker compose exec postgres psql -U postgres -tAc 'select count(*) from entries'` → `1001`.
- [ ] `ss -tn`/`ss -un` em gateway, core e ledger: TCP :8443 no h2 e UDP :8443 no h3, uma conexão por par.
- [ ] Com `NETEM_DELAY=5`: `tc qdisc show` no core mostra netem só na interface da `rpcnet`, e nada na da `datanet`.
- [ ] `docker stats` durante o smoke mostra cada contêiner no seu cpuset (conferido com `docker inspect --format '{{.HostConfig.CpusetCpus}}'`).

### Fase P3 — Runner, análise e trace

- `run.sh` (§10), `analyze.py` (validade por salto, `rpc_cpu`), `make trace`, `timeline.py`, testes pytest.

**Aceite:**
- [ ] `pytest analysis` passa no Windows, com testes novos:
  - repetição com um `svc-*.json` com `dials = 2` → descartada com aviso;
  - sem `svc-*.json` → regra antiga;
  - `rpc_cpu` = soma das três colunas;
  - `timeline.py` ordena e agrupa linhas sintéticas de 3 contêineres por `payment_id`.
- [ ] Na VM, `make test-py` passa.
- [ ] `make trace TRANSPORT=h3`: `timeline.txt` com 21 pagamentos (20 + probe). Cada um tem as linhas das três camadas de todos os serviços, `http=HTTP/3.0` em toda linha `rpc` de cliente, e `risk result received` **depois** de `payment response`.
- [ ] `SCENARIOS='*' REPS=1 make run-all` roda a matriz inteira sem intervenção. Cada repetição tem `requests.csv`, `meta.json`, `stats.csv`, os 6 logs e os 4 `svc-*.json`; o s5 tem `events.csv`.
- [ ] `make analyze RUN=<id>` gera `summary.csv`, `comparison.csv` e os gráficos, com `rpc_cpu`.

### Fase P4 — Calibração

- `make calibrate`; escolha das taxas pela §9; atualização do `LOADGEN_ARGS` nos `.env`.

**Aceite:**
- [ ] Tabela da varredura (taxa × transporte: `served_frac`, `late_starts`, p50, CPU por contêiner) registrada em `doc/PAYMENT-RUN-ANALYSIS.md` §Calibração, com a taxa alta, a baixa, o prazo e a decisão sobre o s2 de capacidade.
- [ ] `make fitness` passa. Depois, com as taxas escolhidas, `SCENARIOS=s1-baseline REPS=3` no h3: todo contêiner ≤ 70% de CPU, p50 ≤ 1,5× o da menor taxa da varredura, `degraded_s = 0` e nenhuma repetição com `cpu_outlier`.
- [ ] `SCENARIOS=s2-loss-5-low REPS=3` no h2: `served_frac ≥ 0,99` e erros ≤ 1%.

### Fase P5 — Rodada completa e documentação

- Duas rodadas de `REPS=30` (juntas, 60 por célula, 50 no mínimo), depois `make analyze` juntando as rodadas.
- Resultados em `doc/PAYMENT-RUN-ANALYSIS.md`: efeito do transporte por cenário (a tabela POC × sistema da rev. 2 saiu na reorganização de 2026-10-20).
- `architecture.md` reescrito para o sistema; o material do POC foi para `archive/poc/`.

**Aceite:**
- [x] Nenhuma repetição no `failures.log` além das que a nova tentativa do probe resolveu; `dials = 1` em todos os saltos e `recorder_stalls = 0` em todas (720 de 720).
- [x] `events_dropped = 0` em todas as repetições sem apagão (0 em todas, inclusive no s5).
- [x] `cv_p50 < 0,10` no s1, nos `-low` e no s5 (o s3 do h3 pode passar, como no POC; registrar): máximo de 0,074, e o s3 também passa.
- [x] Sanidade: o p99 do h2 cresce de forma monotônica de 1% para 5% nos `-low`; o s5 mostra a queda e a retomada, com `recovery_s` calculado (p99 do h2: 84 → 261 → 446 ms; `recovery_s` nas 120 repetições do s5).
- [x] As medianas das duas rodadas batem (mesmo critério usado no POC): `PAYMENT-RUN-ANALYSIS.md` §1.

## 13. Fora do escopo desta fase

- **Cenário 4 (migração de IP do cliente):** fase própria, logo depois desta, no salto loadgen→gateway, com spec própria. Exige acrescentar à API do `transport` (acesso à `quic.Conn` e um novo `quic.Transport` para `AddPath`/`Probe`/`Switch`), IP secundário no loadgen, uma métrica de interrupção e a definição da semântica do h2 (o TCP cai, e a reconexão viola `dials = 1`).
- Falha num único salto (borda × interno), consistência sob timeout (pagamento liquidado com erro no cliente), estornos, crédito ao lojista, contas disputadas.
- Novas tentativas, *circuit breaker*, *publisher confirms*, mensagens persistentes.
- Streaming, Kubernetes/kind, Prometheus/Grafana, OpenTelemetry, 0-RTT explícito.

## 14. Riscos e mitigação

| Risco | Mitigação |
|---|---|
| CPU insuficiente: Gateway e Core são servidor e cliente (~2× o custo por RPC do POC); o h3 custa +11% no servidor e +30% no cliente | Calibração com h3 (pior caso), teto de 70% por contêiner, CPU por contêiner no `reps.csv` |
| Host com 1 vCPU (no POC eram 2): `docker stats`, `dockerd` e o `run.sh` disputam o núcleo 0 | `stats.csv` mostra a saturação. Se o `degraded_s` ou o `cv_p50` do s1 piorarem em relação ao POC, rever: loadgen e host dividindo `0-1` |
| Núcleo 5 dividido: postgres, rabbitmq e risk causam variância no Ledger | Igual nos dois transportes; Erlang sem *busy-wait*; CPU do núcleo 5 no `stats.csv` |
| Ruído de host ([`../archive/poc/todo.md`](../archive/poc/todo.md) itens 12–13) | vCPU reservada no Hyper-V e host parado durante as rodadas; `make fitness` antes de toda rodada longa (§9); `cpu_outlier` na análise para ruído em repetições isoladas |
| A perda acumula em 3 saltos: caudas maiores e mais erros por prazo | Taxa baixa e prazo confirmados na P4 (§9); `p*_all` mostra as falhas |
| O probe abre 3 conexões em série sob perda e pode estourar o prazo de 7 s com mais frequência | A nova tentativa do `run.sh` continua; se passar de 1% das repetições, aumentar o prazo do probe nos dois transportes |
| A perda falsa do quic-go v0.63 (s3) acontece em cada salto | É resultado, não defeito. A pergunta da fase é justamente se ela se amplia; `QLOG=1` serve para diagnóstico. **Desfecho:** a cauda não apareceu a 300 RPC/s (`PAYMENT-RUN-ANALYSIS.md` §5) |
| Cliente h2 do Go trava com mais de 1000 streams numa conexão | Invariante fan-out = 1 e `--concurrency ≤ 1000`; um teste da P1 garante 1 RPC por salto por pedido |
| Broker lento bloqueando o Core | Fila em memória sem bloqueio, descarte contado em `events_dropped`, que precisa ficar 0 |
| Certificados do POC sem os novos nomes | `rm -rf certs && make certs` é o primeiro passo da P2 |
| `synchronous_commit=off` questionado no texto | Justificar: a durabilidade não está sob teste, e o fsync poria o disco da VM no caminho medido |

## 15. Versões fixadas

Herdadas do POC ([`../archive/poc/plan/poc-plan.md`](../archive/poc/plan/poc-plan.md) §11): Go 1.27.1, quic-go v0.63.0, connect-go v1.21.0, protobuf v1.36.12, buf v1.73.0, imagens `golang:1.27.1-trixie`, `debian:trixie-slim` e `python:3.13-slim-trixie`, bibliotecas Python.

Novas, fixadas no início da Fase P2 com a última versão estável (no `go.mod` e no `compose.yaml`, por tag de versão completa) e congeladas durante as rodadas:

| Item | Versão |
|---|---|
| `github.com/jackc/pgx/v5` | v5.11.0 |
| `github.com/rabbitmq/amqp091-go` | v1.15.0 |
| PostgreSQL | `postgres:18.6-trixie` |
| RabbitMQ | `rabbitmq:4.3.6` (sem `-management`) |

## Referências técnicas

- TCC, §3.2 (cenários), §3.3 (domínio da aplicação) e Figura 5.
- [`../architecture.md`](../architecture.md), [`../PAYMENT-RUN-ANALYSIS.md`](../PAYMENT-RUN-ANALYSIS.md); POC (histórico): [`../archive/poc/`](../archive/poc/).
- connect-go — propagação de prazo (`grpc-timeout`) — https://connectrpc.com/docs/go/deadlines/
- RabbitMQ — *runtime tuning* (escalonadores e *busy-wait* do Erlang) — https://www.rabbitmq.com/docs/runtime
- PostgreSQL — `synchronous_commit` — https://www.postgresql.org/docs/current/wal-async-commit.html
