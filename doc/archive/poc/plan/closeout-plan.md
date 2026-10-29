# POC parte 1 — ajustes de fechamento

> Continua o `implementation-plan.md` (Fases 1 e 2 prontas). Spec: `poc-plan.md`. Mesmo fluxo: TDD, ledger, revisão no fim. Formato enxuto.

Três mudanças antes da revisão e da documentação completa:

1. log por RPC no servidor, como base para os microsserviços da próxima fase;
2. pasta do run com nome só de timestamp;
3. mais métricas na análise, sem novos gráficos.

## Restrições

- Valem as restrições globais do `implementation-plan.md`.
- Uma delas muda: "servidor sem log por requisição" vira "sem log por requisição **por padrão**". O log por RPC fica no nível `debug` e o padrão é `info`, então os números do benchmark não mudam.
- Logs no formato JSON do `slog`, como já é hoje.

---

### Tarefa A: log por RPC (`internal/rpclog`)

**Arquivos:**
- criar `internal/rpclog/rpclog.go` e `internal/rpclog/rpclog_test.go`;
- modificar `cmd/server/main.go` e `cmd/server/main_test.go`;
- atualizar a §5.3 do spec e a restrição global do `implementation-plan.md`.

**Produz:**
```go
// Interceptor logs one line per unary RPC: procedure, code, duration_us, peer, protocol.
func Interceptor(log *slog.Logger) connect.Interceptor
```

**Comportamento:**
- Mede o tempo do handler.
- `code` é `ok` quando não há erro; senão, `connect.CodeOf(err).String()`.
- Sucesso e falha são registrados em `debug` (decisão D1, do usuário).
- Quando o nível está desligado, sai cedo com `log.Enabled`: não monta atributos e não aloca.
- Não altera o erro nem a resposta.

**Servidor:**
- Nova flag `--log-level` e env `LOG_LEVEL`, com valores `debug|info|warn|error` e padrão `info`.
- A precedência é a mesma de hoje: flag > env > padrão. Valor inválido falha na inicialização, citando a flag e a env.
- `newMux(log)` passa a registrar `connect.WithInterceptors(rpclog.Interceptor(log))` no handler do BenchService. O `/healthz` fica sem log.

**Testes (TDD):**

| Teste | Esperado |
|---|---|
| RPC ok com nível `debug` | uma linha JSON com `msg=rpc`, `procedure=/bench.v1.BenchService/Unary`, `code=ok` e `duration_us` ≥ 0 |
| RPC com erro (`response_size` > máximo) | uma linha com `code=invalid_argument`; o cliente recebe o mesmo erro |
| nível `info` | RPCs ok não geram linha |
| `LOG_LEVEL=verbose` | `run` falha antes de abrir a porta, e a mensagem cita `LOG_LEVEL` |

---

### Tarefa B: pasta do run com timestamp (`scripts/run.sh`)

**Arquivos:** modificar `scripts/run.sh` e a §6.4 do spec.

- `run_id=$(date +%Y-%m-%d-%H-%M-%S)`, na hora local da VM (decisão D2, do usuário). O commit continua registrado em `meta.json.version`.
- Se `results/<run_id>` já existir, o script para com erro, porque resultado é imutável.
- Sem teste automatizado (script só roda na VM). Verificação na VM: `SCENARIOS=s1-baseline REPS=1 make run-all` gera `results/2026-..-..-..-..-../`.

---

### Tarefa C: análise — métricas extras (`analysis/analyze.py`)

**Arquivos:** modificar `analysis/analyze.py` e `analysis/tests/test_metrics.py`.

Novas colunas por repetição, que entram no `summary.csv` com `_mean` e `_ci95`:

| Coluna | Definição | Para quê |
|---|---|---|
| `handshake_ms` | `meta.handshake_us / 1000` (RPC de teste que abre a conexão) | custo de conexão: TCP + TLS 1.3 (2 RTT) vs QUIC (1 RTT) |
| `cpu_per_krps` | `server_cpu / (ok_rps / 1000)` | CPU do servidor por 1000 RPC/s; com taxa fixa, compara a eficiência das pilhas |
| `degraded_s` | nº de bins de 1 s fora do apagão com vazão ok < 90% do `--rps` (NaN em loop fechado); o primeiro bin e os bins depois do último envio são parciais e não contam | no s1 e no s5, marca repetições com ruído do ambiente, como os travamentos vistos no s5; no s2/s3 também pode ser efeito do transporte; o `analyze` avisa quando > 0 |

No `degraded_s`, a janela do apagão vai de `outage_start` até `outage_end + recovery_s`. Quando não há recuperação, conta até o fim do run.

Novo `analysis/comparison.csv`, com uma linha por cenário × métrica (`p50`, `p99`, `p999`, `error_rate`, `recovery_s`, `cpu_per_krps`, `handshake_ms`). Colunas:
- `h2_mean`, `h3_mean`;
- `delta_pct = (h3 − h2) / h2 × 100`;
- `mwu_p`, o Mann-Whitney bilateral sobre os valores por repetição;
- `n_h2`, `n_h3`.

É a tabela que vai direto para o texto do TCC. O `summary.csv` não muda de formato, exceto pelas colunas novas.

**Testes (TDD):**

| Teste | Esperado |
|---|---|
| `handshake_ms` e `cpu_per_krps` | meta com `handshake_us=4000`; `server_cpu=50` com `ok_rps=100` → `4.0` e `500.0` |
| `degraded_s` | rps=100; bins 100, 100, 50, 100 → `1`; um bin baixo dentro do apagão não conta; rps=0 → NaN |
| `comparison` | h2 p99 `[10,12,14]`, h3 `[7,8,9]` → `delta_pct = −33.33`, `mwu_p = 0.1` |
| `main` | gera `comparison.csv` com uma linha por cenário × métrica presente |

---

### Fim

- `go vet ./...`, `go test ./...` e `pytest analysis` verdes, com a saída mostrada.
- Revisão independente do diff inteiro; corrigir o que for aceito.
- Comandos para a VM (Tarefa B e `make analyze`).
- Mensagens de commit (você faz): `Add per-RPC debug log interceptor`, `Name run folders by timestamp` e `Add handshake, CPU cost, degraded time and h2/h3 comparison to analysis`.
