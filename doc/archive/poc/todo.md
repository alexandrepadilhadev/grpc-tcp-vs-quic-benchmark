# TODO — fechamento do POC

Origem: `doc/RUN-ALL-ANALYSIS.md` §8 — itens 1–10 da rodada de 2026-09-28; itens 11–14 da análise de 2026-10-07 (duas rodadas completas, `7f84f30`, 50 repetições por transporte).

## Status

| Item | Status |
|---|---|
| 1. s3 — reordenação do netem | ✅ feito; verificado com 50 repetições (2026-10-07): 100% da demanda nos dois |
| 2. s2 — taxa baixa | ✅ feito; verificado com 50 repetições (2026-10-07): 100% da demanda nos dois |
| 3. `served_frac` | ✅ feito (`9c8be8b`), testes passando |
| 4. Mediana e Hodges–Lehmann | ✅ feito |
| 5. Delta de Cliff | ✅ feito |
| 6. Holm | ✅ feito (família por cenário) |
| 7. `--exclude-degraded` | ✅ feito |
| 8. Juntar rodadas | ✅ feito |
| 9. `p*_all` | ✅ feito, com M7 corrigido no loadgen; falhas por `max(latência, prazo)` |
| 10. Razão h3/h2 com IC | ✅ feito |
| Extra — nova tentativa do probe no `run.sh` | ✅ feito; agiu 1 vez em 900 repetições (2026-10-07) |
| 11. s3 — causa da cauda do h3 (qlog) | ✅ feito; causa confirmada (`RUN-ALL-ANALYSIS.md` §5.1) |
| 12. Harness — ruído de host no início da rodada | dispensado: com a vCPU reservada no Hyper-V e o host parado, a 1ª repetição da rodada já sai limpa (diagnóstico de 2026-10-12, P4 do sistema de pagamentos) |
| 13. Análise — detector de ruído por CPU | ✅ feito: coluna `cpu_outlier` (mediana + 3 MAD do `cpu_per_krps` por rodada × cenário × transporte, e ao menos +20% sobre a mediana), aviso, e `--exclude-degraded` também a exclui |
| 14. Texto do TCC — tabelas e figuras | pendente |

Itens 4–10: testes em `analysis/tests/test_metrics.py` e `internal/loadgen/runner_test.go`; colunas documentadas em `architecture.md` §6.2 e §6.6. Falta rodar `make test` e `make test-py` na VM.

## 1. s3 — corrigir a reordenação do netem ✅

- **Problema:** `netem delay 20ms 10ms` sorteia um atraso por pacote e reordena pacotes enviados com ~0,5 ms de intervalo. As duas pilhas tratam a reordenação como perda e colapsam (h2 8%, h3 0,5% da demanda). O cenário mede reordenação, não jitter.
- **Correção:** em `deploy/netem.sh`, quando `JITTER` > 0, acrescentar `rate 10gbit` ao netem: cada pacote sai no máximo entre o seu sorteio e a saída do anterior (FIFO). A `pfifo` filha proposta antes não funciona, porque o netem só entrega os pacotes a ela depois do atraso de cada um (`sch_netem.c`).
- **Verificar:**
  - `tc qdisc show` no contêiner mostra o netem com `rate 10Gbit`;
  - rodar o s3 de novo na VM;
  - resultado esperado: sem colapso, demanda atendida próxima de 100% nos dois transportes.
- **Feito** (`daec7d1`): `rate 10gbit` no `netem.sh`, documentado em `architecture.md` §5.2 e no `poc-plan.md`.
- **Verificado** na rodada `2026-10-01-20-05-25` (1 repetição): o `server.log` mostra `rate 10gbit`; demanda atendida de 99,999% no h2 e 100% no h3, sem erros (antes: 6,7% e 0,2%).
  - p50 de ~75 ms nos dois, quase o dobro do RTT base de 40 ms: com a fila FIFO, cada pacote espera o mais atrasado dos sorteios recentes.
  - p99 do h3 5× o do h2 (519 contra 97 ms); confirmar com 30 repetições.

## 2. s2 — variante com taxa baixa (latência e HoLB) ✅

- **Problema:** a 2000 RPC/s o s2 satura (h2 em todos os níveis, h3 a 3% e 5%), e a latência vira tempo de fila.
- **Mudança:**
  - manter o s2 atual como "capacidade sob perda" (comparar vazão e demanda atendida);
  - criar novos `experiments/scenarios/s2-loss-{1,3,5}-low.env` com `--rps=200`, abaixo da capacidade do h2 a 5% (~332 RPC/s medidos);
  - ajustar `--concurrency` pela regra workers ≥ rps × latência, respeitando ≤ 1000 (limite de streams do h2).
- **Verificar:** demanda atendida ≈ 100% nos dois transportes em todos os níveis; aí o p50/p99 mede HoLB, e não fila.
- **Feito** (`9c8be8b`): `s2-loss-{1,3,5}-low.env` com `--rps=200 --concurrency=500`; tabelas de cenários do `architecture.md` §5.2 e do `poc-plan.md` atualizadas.
- **Pendente:** rodar na VM (`SCENARIOS='s2-loss-*-low' REPS=30 make run-all`) e conferir a demanda atendida.

## 3. Análise — demanda atendida (`served_frac`) ✅

- **Problema:** slots não enviados (`unsent`) não aparecem na análise; o `error_rate` fica em ~0 mesmo com 50–99% da demanda perdida.
- **Mudança** em `analysis/analyze.py`:
  - nova coluna `served_frac = ok / (rps × duration)`, lida do `meta.json`;
  - entra no `reps.csv`, no `summary.csv` (`_mean`, `_ci95`) e no `comparison.csv`;
  - `NaN` em loop fechado (`rps = 0`).
- **Testes (TDD)** em `analysis/tests/test_metrics.py`:
  - rps=100, duration=60 s, ok=3000 → `0.5`;
  - rps=0 → `NaN`;
  - `main` grava `served_frac` no `comparison.csv`.
- **Feito** (`9c8be8b`): função `served_frac(meta)` em `analyze.py`, coluna no `reps.csv`, `summary.csv` e `comparison.csv`; documentada em `architecture.md` §6.2. Conferida nas rodadas da VM.

## 4. Análise — mediana e intervalo de confiança não paramétricos ✅

- **Problema:**
  - hoje o `summary.csv` e o `comparison.csv` só têm média ± IC t de Student;
  - com 5 repetições, valores assimétricos (percentis de latência, s5 bimodal) e repetições ruidosas, a média e o IC t são frágeis: uma repetição com ruído de host puxa a média;
  - o Mann-Whitney já é não paramétrico, mas falta uma estimativa do tamanho da diferença com IC na mesma família.
- **Mudança** em `analysis/analyze.py`:
  - `summary.csv`, por cenário × transporte e métrica:
    - `<m>_median`;
    - `<m>_med_lo` e `<m>_med_hi`: IC da mediana sem suposição de distribuição, pelas estatísticas de ordem (binomial, p = 0,5);
    - nível real do IC (o maior ≤ 95% possível com o n disponível; com n = 5, o IC é [mín, máx] com 93,75%).
  - `comparison.csv`, por cenário × métrica:
    - `h2_median`, `h3_median`;
    - `hl_shift`: estimador de Hodges–Lehmann (mediana das diferenças h3 − h2 entre todos os pares);
    - `hl_lo` e `hl_hi`: IC exato do deslocamento, derivado da distribuição do U de Mann-Whitney;
    - nível real do IC (com 5 × 5, ≈ 96,8%).
  - Manter as colunas de média e IC t, para não quebrar o que já existe.
- **Testes (TDD)** em `analysis/tests/test_metrics.py`:
  - mediana de `[1, 2, 3, 4, 100]` → `3`, IC `[1, 100]`, nível `0.9375`;
  - Hodges–Lehmann com h2 `[10, 12, 14]` e h3 `[7, 8, 9]` → `hl_shift = -4`;
  - n = 1 → IC `NaN`, sem erro.
- **Feito:** `median_ci()` e `hodges_lehmann()`. O IC é o mais estreito com cobertura ≥ 95%; sem isso, o intervalo inteiro (n = 5 → [mín, máx], 93,75%). Coluna de nível: `<m>_med_level` e `hl_level`. Com 30 × 30, o nível real é 95,04%.

## 5. Análise — tamanho de efeito (delta de Cliff) ✅

- **Problema:** com 5 × 5, toda separação total dá p = 0,008, o menor p possível. O p diz que existe diferença, não quão separados estão os grupos.
- **Mudança** em `comparison()`:
  - nova coluna `cliff_delta = (#pares h3 > h2 − #pares h3 < h2) / (n_h2 × n_h3)`, de −1 a 1;
  - nova coluna `cliff_mag` com a classificação de Romano et al. (2006): |δ| < 0,147 desprezível, < 0,33 pequeno, < 0,474 médio, senão grande.
- **Testes (TDD):**
  - h2 `[10, 12, 14]`, h3 `[7, 8, 9]` → `-1.0`, `large`;
  - grupos iguais → `0.0`, `negligible`.
- **Feito:** `cliff_delta()` e `cliff_magnitude()`; colunas `cliff_delta` e `cliff_mag` no `comparison.csv`.

## 6. Análise — correção para comparações múltiplas (Holm-Bonferroni) ✅

- **Problema:** o `comparison.csv` faz ~7 métricas × 7 cenários ≈ 50 testes. A 5%, uns 2 ou 3 "significativos" aparecem por acaso.
- **Mudança:**
  - nova coluna `mwu_p_holm`, com a correção de Holm aplicada **por cenário**, sobre as métricas daquele cenário;
  - valores `NaN` (empate total, minor M1 do fechamento) ficam fora da família e continuam `NaN`.
- **Decisão em aberto:** família por cenário (cada cenário é uma pergunta) ou o arquivo inteiro (mais conservador). Padrão proposto: por cenário.
- **Testes (TDD):**
  - p `[0.01, 0.04, 0.03]` → `[0.03, 0.06, 0.06]`;
  - p com `NaN` → o `NaN` se mantém e não entra na contagem.
- **Feito:** `holm()`; coluna `mwu_p_holm`, com a família **por cenário** (padrão proposto).

## 7. Análise — excluir repetições com ruído de host ✅

- **Problema:** o `degraded_s > 0` só gera aviso. O resumo, o `latency_cdf.png` e o `p99_boxplot.png` incluem as repetições ruidosas. O `RUN-ALL-ANALYSIS` já recomendava não usar o CDF do s1 por isso.
- **Mudança:**
  - nova flag `--exclude-degraded=<cenário>,…` (ex.: `s1-baseline,s5-outage`), só para cenários sem perda ou jitter injetados, onde `degraded_s > 0` indica ruído de host e não efeito do transporte;
  - o `reps.csv` mantém todas as repetições, com a nova coluna `excluded` (bool);
  - o `summary.csv`, o `comparison.csv` e os gráficos usam só as não excluídas;
  - o `summary.csv` ganha `n_excluded`, e a análise avisa quantas saíram por cenário × transporte.
- **Testes (TDD):**
  - 3 repetições, uma com `degraded_s = 2` e a flag ativa para o cenário → `n = 2`, `n_excluded = 1`;
  - mesmo caso com a flag para outro cenário → nenhuma exclusão.
- **Feito:** `--exclude-degraded` no `analyze.py` e `EXCLUDE=` no `make analyze`; coluna `excluded` no `reps.csv`, `n_excluded` no `summary.csv`; CDF, boxplot e s5_throughput usam só as não excluídas.

## 8. Análise — juntar várias rodadas ✅

- **Problema:** a análise lê uma única pasta de rodada. O s1 teve 2 rodadas, o s5 deveria se juntar à rodada de 180 s, e as 30 repetições do TCC provavelmente serão divididas em várias rodadas.
- **Mudança:**
  - `analyze.py <run_dir> [<run_dir> …] [--out <dir>]`;
  - com uma pasta, o comportamento atual se mantém (`<run_dir>/analysis`); com várias, `--out` é obrigatório;
  - nova coluna `run` no `reps.csv`; a repetição passa a ser identificada por (`run`, `rep`);
  - aviso quando a configuração de carga (`meta.load`) ou o commit (`meta.version`) diferem entre rodadas do mesmo cenário;
  - ajustar o `make analyze` para aceitar várias pastas.
- **Testes (TDD):**
  - 2 rodadas com 2 repetições cada no mesmo cenário → `n = 4` no resumo;
  - várias pastas sem `--out` → código de saída 2 e mensagem de uso;
  - rodadas com `rps` diferentes → aviso.
- **Feito:** `analyze.py <run>... --out <dir>` e `make analyze RUN="a b" OUT=<dir>`; coluna `run` no `reps.csv` (também com uma pasta só); aviso `<cenário>: load differs between runs` ou `commit differs between runs`.

## 9. Análise — latência contando as falhas ✅

- **Problema:** os percentis usam só as RPCs ok (viés de sobrevivência). Se um transporte falha mais ou deixa mais slots sem enviar, o p99 "ok" fica melhor do que a experiência real.
- **Mudança:**
  - novas colunas por repetição `p50_all`, `p99_all` e `p999_all`;
  - nelas, cada RPC com erro e cada slot não enviado (`unsent`) entram com a latência igual ao prazo da RPC (2 s, lido do `meta.json`; falhar com mensagem clara se o campo não existir);
  - entram no `summary.csv` e no `comparison.csv`.
- **Dependência:** o `unsent` hoje inclui os slots do aquecimento (minor M7 da Fase 1). Corrigir isso no loadgen ou separar `unsent` por fase no `meta.json` antes desta tarefa.
- **Testes (TDD):**
  - 98 RPCs ok a 10 ms + 2 erros, prazo 2 s → `p99_all = 2000 ms`, enquanto o `p99` atual fica em 10 ms;
  - sem erros nem `unsent` → `p99_all == p99`.
- **M7 corrigido** em `internal/loadgen/runner.go`: `unsent` e `late_starts` contam só os slots da medição (testes `TestUnsentCountsMeasureSlotsOnly` e `TestLateStartsCountMeasureSlotsOnly`). Rodadas antigas continuam com a contagem antiga.
- **Ajuste na especificação:** nos cenários saturados, as RPCs ok ficam até 60 s na fila, e entrar com as falhas em 2 s **baixava** a cauda (s3 h3: p99 61 s, `p99_all` 2 s). Decisão: erros entram com `max(latência, prazo)` e os `unsent` com o prazo; como os `p*`, os `p*_all` só valem sem saturação (`served_frac` ≈ 1).
- **Feito:** `latency_all()`; colunas `p50_all`, `p99_all`, `p999_all` no `reps.csv`, `summary.csv` e `comparison.csv`; sem `load.deadline_ns` no `meta.json`, o `analyze` para com erro.

## 10. Análise — razão h3/h2 com IC para vazão ✅

- **Problema:** o texto afirma "o h3 atende 2,88× mais", mas não há IC para essa razão.
- **Mudança:**
  - para as métricas de vazão (`ok_rps`, `served_frac`), aplicar o Hodges–Lehmann do item 4 sobre o **log** dos valores por repetição;
  - novas colunas no `comparison.csv`: `ratio = exp(hl_shift)`, `ratio_lo` e `ratio_hi`; `NaN` nas demais métricas.
- **Dependência:** item 4 (Hodges–Lehmann) e item 3 (`served_frac`).
- **Testes (TDD):**
  - h2 `[100, 100, 100]`, h3 `[200, 200, 200]` → `ratio = 2.0`;
  - valor ≤ 0 em algum grupo → `NaN` e aviso.
- **Feito:** colunas `ratio`, `ratio_lo`, `ratio_hi` para `ok_rps` (agora também em `COMPARE`) e `served_frac`. Nas rodadas de 2026-10-01, o s2-loss-3 dá `ok_rps` 2,59× [2,56–2,63].

---

## 11. s3 — confirmar a causa da cauda do h3 (qlog) ✅

- **Problema:** no s3 (jitter 20 ms ± 10 ms, netem FIFO), o h3 tem p50 7 ms menor, mas p99 4× maior que o h2 (421 contra 96 ms), muito variável entre repetições (91 ms a 2,5 s). Em 35 de 50 repetições, o h3 tem segundos com vazão abaixo de 90%; o h2, nenhum.
- **Hipótese:** as rajadas de atraso passam do limiar de tempo da detecção de perda do QUIC (9/8 do RTT), e o quic-go declara perdas que não aconteceram: retransmite e reduz a janela. O TCP do Linux adapta o limiar (RACK) e sofre menos.
- **Mudança:**
  - em `internal/transport`, ligar o qlog do quic-go só quando a variável `QLOGDIR` estiver definida (`Tracer: qlog.DefaultConnectionTracer`); sem a variável, nada muda nas medições;
  - em `compose.yaml`, repassar `QLOGDIR` ao server e ao loadgen, gravando em `results/<run>/…`;
  - rodar 1 repetição do s3 com h3 e qlog ligado.
- **Verificar:**
  - nos eventos `packet_lost` do qlog, contar quantos têm gatilho `time_threshold` e quantos `reordering_threshold`;
  - se quase todos forem `time_threshold`, a hipótese se confirma; para o TCP, comparar com `nstat` (`TcpExtTCPSpuriousRTOs`, `TcpExtTCPLossProbes`) numa repetição h2.
- **Opcional:** variante `s3-jitter-low.env` com ±5 ms, para ver se a cauda some quando as rajadas ficam abaixo do limiar.
- **Feito:**
  - `internal/transport/qlog.go`: `Tracer = qlog.DefaultConnectionTracer` no cliente e no servidor h3, só com `QLOGDIR` definido; testes `TestQuicTracerOffByDefault` e `TestQlogDir`;
  - `run.sh`: `QLOG=1` define `QLOGDIR=<rep>/qlog` nos dois contêineres e grava `nstat -az` do servidor em `<rep>/nstat.txt`; `compose.yaml` repassa `QLOGDIR` e monta `results/` no servidor;
  - documentado em `architecture.md` §7.
- **Pendente (VM):** rodar `QLOG=1 SCENARIOS=s3-jitter REPS=1 make run-all` e contar as perdas:
  - QUIC: `grep -h '"recovery:packet_lost"' <rep>/qlog/*.sqlog | grep -o '"trigger":"[a-z_]*"' | sort | uniq -c`;
  - TCP: `grep -E 'TcpRetransSegs|TCPDSACKRecv|TCPSpuriousRTOs|TCPLossProbes|TCPSACKReorder' <rep>/nstat.txt`.
  - Os arquivos `.sqlog` passam de 100 MB por lado numa repetição de 70 s; apagar depois de contar.
- **Resultado** (rodada `2026-10-07-21-41-01`, 1 repetição; detalhes em `RUN-ALL-ANALYSIS.md` §5.1):
  - todas as perdas declaradas pelo QUIC foram falsas (40 declaradas, 43 `spurious_loss`); a maioria por limiar de **reordenação** (29), não de tempo (11);
  - 2 delas, no servidor, reduziram a janela (~200 → 145 KB), e o quic-go não a restaurou ao detectar o engano; com a janela abaixo do produto banda × atraso por ~5 s, as respostas fazem fila, e essas duas janelas concentram toda a cauda do p99;
  - o TCP viu a mesma reordenação (87 `TCPSACKReorder`) e retransmitiu só 5 segmentos, sem RTO falso.
- **Em aberto (opcional):** origem da reordenação residual com o netem em FIFO; s3 com ±5 ms; comportamento de versões mais novas do quic-go.

## 12. Harness — ruído de host no início da rodada

- **Problema:** nas duas últimas rodadas, os primeiros ~25 min tiveram contenção de CPU no host. No s1 da rodada A (repetições 2–10), o CPU do servidor subiu 20–40% e o p50 até 2×. O s1 dessa rodada precisou ser descartado na análise.
- **Mudança:** no `run.sh`, antes da matriz, rodar uma repetição de descarte do s1 em `results/<run>/_warmup/` (fora do glob `*/*/rep-*` da análise). Opcional: `WARMUP_REPS=<n>` (padrão 1; 0 desliga).
- **Decisão em aberto:** uma repetição de ~75 s basta? Pelas rodadas, o ruído durou ~25 min; talvez seja melhor checar o host (`vmstat`, `steal` e `us` do host) antes de começar, em vez de um tempo fixo.
- **Verificar:** numa rodada nova, o `cpu_per_krps` do s1 nas primeiras repetições fica na mesma faixa das últimas.

## 13. Análise — detector de ruído por CPU

- **Problema:** o `degraded_s` só pega ruído que derruba a vazão. A contenção de CPU no início da rodada A manteve a vazão e passou despercebida: o detector marcou 1 de 9 repetições afetadas.
- **Mudança** em `analysis/analyze.py`:
  - nova coluna `cpu_outlier` no `reps.csv`: `cpu_per_krps` acima da mediana + 3 × MAD do mesmo cenário × transporte × rodada;
  - aviso no terminal, como o do `degraded_s`;
  - `--exclude-degraded` passa a excluir também `cpu_outlier` nos cenários indicados.
- **Testes (TDD):**
  - 10 repetições com `cpu_per_krps` ≈ 40 e uma com 60 → só essa é marcada;
  - todas iguais (MAD = 0) → nenhuma marcada, sem divisão por zero.
- **Verificar:** na rodada `2026-10-06-23-17-30`, as repetições 2–10 do s1 são marcadas, e as da rodada B não.

## 14. Texto do TCC — tabelas e figuras

- Nas tabelas: mediana, deslocamento HL com IC e delta de Cliff; para vazão, razão h3/h2 com IC. O p corrigido por Holm entra só como confirmação.
- s1: tirar das rodadas limpas (rodada B + `2026-10-01-23-09-19`, 80 repetições) e destacar a relevância prática (efeitos < 1 ms) além da significância.
- s2: separar **latência** (`-low`, 200 RPC/s) de **capacidade** (2000 RPC/s); a latência a 2000 RPC/s é tempo de fila.
- s5: descrever a distribuição da recuperação (h2 sempre 1 s; h3 4–5 s em 90%, 0–1 s em 10%), não só a média.
- Figuras: CDF da rodada B (`results/2026-10-07-06-59-47/analysis/latency_cdf.png`) e `s5_throughput` do pooled (`results/pooled-2026-10-07/s5_throughput.png`).

---

**Ordem sugerida:** ~~1 a 10~~ (feitos) · 11 (independente; muda o código de transporte, então rodar o s1 de novo depois para confirmar que nada mudou com o qlog desligado) · 13 → 12 (o detector ajuda a medir se o aquecimento funcionou) · 14 depois de 11.
