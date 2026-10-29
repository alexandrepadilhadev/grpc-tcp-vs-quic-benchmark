# Análise dos resultados — sistema de pagamentos (atualizada em 2026-10-17)

Resultados da fase do sistema de pagamentos (spec [`plan/payment-plan.md`](plan/payment-plan.md)): gateway → core → ledger, três saltos gRPC síncronos, cada um com uma conexão, todos trocando juntos entre h2 e h3. Duas rodadas completas da matriz, no mesmo commit (`d83af8e`), na VM (Ubuntu, 6 vCPUs, vCPU reservada no Hyper-V). Juntas, dão **60 repetições por transporte em cada um dos 6 cenários**. Carga: `--rps=300 --concurrency=500 --duration=60s --warmup=10s`, com o prazo padrão de 2 s, no s1, no s3 e no s5; `--rps=200` nos `-low`. Mensagens de 40 B (pedido) e 44 B (resposta). A calibração que fixou essas taxas está na [última seção](#calibração).

**Convenções:**
- Valores são **medianas entre repetições**. Latências em ms.
- **Δ (HL):** deslocamento de Hodges–Lehmann h3 − h2, com IC exato de ~95% entre colchetes.
- **p:** Mann-Whitney U bilateral, **corrigido por Holm** dentro do cenário (`mwu_p_holm`).
- **δ:** delta de Cliff, de −1 (todo h3 < todo h2) a +1. |δ| ≥ 0,474 é efeito grande.
- **RTT de salto:** 10 ms no s2 e no s5 (5 ms por lado), 40 ms de base no s3. A RPC atravessa 3 saltos, então o RTT base de ponta a ponta é 30 ms e 120 ms.

## Resumo

**Pergunta:** qual é o efeito do transporte (TCP+HTTP/2 × QUIC+HTTP/3) num sistema de pagamentos com 3 saltos gRPC síncronos, em cada condição de rede? A resposta depende da condição: o h3 vence com folga sob perda e sob jitter, o h2 é um pouco mais rápido na rede ideal, e a recuperação de um apagão empata.

1. **Perda com carga baixa (s2-low, 200 RPC/s): o h3 vence em todos os níveis, e a diferença cresce com a perda.** Os dois atendem 100% da demanda:

   | Perda | p50 h2 / h3 | p99 h2 / h3 | Δ p99 (HL) [IC] |
   |---|---|---|---|
   | 1% | 39,8 / 33,5 ms | 84 / 49 ms | −35 [−36; −35] |
   | 3% | 62,2 / 33,6 ms | 261 / 62 ms | −200 [−206; −191] |
   | 5% | 80,3 / 33,8 ms | 446 / 68 ms | −378 [−428; −313] |

   O p50 do h3 fica em ~33,6 ms em qualquer nível, perto do RTT base de 30 ms: uma perda só atrasa o stream do pacote perdido. No h2, cada retransmissão segura todos os streams da conexão, e uma RPC atravessa três conexões; a 5%, o p50 dobra e o p99 fica 6,5× o do h3.
2. **Rede ideal (s1): o h3 é um pouco mais lento.** p50 +0,69 ms (2,70 → 3,37 ms) e p99 +1,37 ms, com δ ≈ +0,9. A causa provável é a CPU: o h3 gasta 40% mais CPU por RPC nos três serviços, e cada um deles, menos o ledger, é ao mesmo tempo cliente e servidor QUIC. Em valor absoluto, a diferença fica abaixo de 1,5 ms.
3. **Jitter (s3): o h3 é mais rápido em toda a distribuição.** −43 ms no p50 e −38 ms no p99 (216 contra 254 ms), com separação total (δ = −1). Nenhuma das 120 repetições tem segundos degradados.
4. **Apagão (s5): a recuperação empata.** `recovery_s` de 2 s nos dois (mediana); o h3 volta em 0 s em 17 das 60 repetições, e o h2 nunca. Depois do apagão, o core do h2 processa ~760 pagamentos por repetição que o cliente já deu como falhos, contra ~40 no h3 (§6).
5. **Handshake: o h3 abre a cadeia 3 RTTs de salto mais rápido**, a diferença que a teoria prevê (h2 ≈ 9 RTTs de salto, h3 ≈ 6): −30,6 ms no s2-loss-1-low e −30,2 ms no s5. Na rede ideal, onde só conta a CPU, o h2 é 2,0 ms mais rápido.

---

## 1. Rodadas usadas

| Pasta em `results/` | Commit | Repetições por transporte | Observação |
|---|---|---|---|
| `2026-10-15-23-55-55` (rodada A) | `d83af8e` | 30, todos os cenários | `make fitness` 183 (limite 200; acima da faixa limpa de 134–172, mas o `cpu_per_krps` do s1 da rodada ficou dentro da referência limpa) |
| `2026-10-16-08-21-08` (rodada B) | `d83af8e` | 30, todos os cenários | `make fitness` 155; ruído leve do host a partir do s1 rep 12 (§7) |
| `pooled-payment-2026-10-16` | — | 60 = A + B | análise conjunta (`analyze.py A B --out …`), base das §3–§7 |
| `pooled-payment-2026-10-16-excl` | — | 60 menos 5 | a mesma, com `--exclude-degraded=s1-baseline,s5-outage`; só para conferir a robustez (§7) |

- **Falhas:** nenhuma das 720 repetições foi para o `failures.log`, e nenhuma precisou da nova tentativa do probe.
- **Integridade:** `dials = 1` no loadgen e nos três saltos (`svc-*.json`) e `recorder_stalls = 0` em todas.
- **Caminho assíncrono:** `events_dropped = 0` em todas, inclusive no s5, e `events_published = risk_results = events_consumed = results_published` em todas.
- **Replicação:** as medianas das rodadas A e B batem em todos os cenários: o efeito h3 − h2 tem o mesmo sinal em toda métrica, e o p50 fica a menos de 1,5% entre as rodadas fora do s1. Exemplos: p99 do s3 no h3, 215 contra 216 ms; p99 do s2-loss-5-low no h2, 465 contra 381 ms (a maior diferença, numa cauda pesada; p = 0,30); `recovery_s`, 2 contra 2 s nos dois transportes. As exceções são explicadas:
  - s1: o Δ do p50 é de 648 µs em A e 810 µs em B, por causa do ruído da rodada B (as 11 primeiras repetições de B dão 678 µs).
  - handshake do s2-loss-3-low e do s2-loss-5-low: quantizado pelas retransmissões do SYN e do Initial (~1 s), varia entre as rodadas.

## 2. Critérios de aceite da Fase P5 (spec §12)

| Critério | Resultado |
|---|---|
| Nenhuma repetição no `failures.log`; `dials = 1` em todos os saltos; `recorder_stalls = 0` | ✅ 720 de 720 |
| `events_dropped = 0` nas repetições sem apagão | ✅ 0 em todas, inclusive no s5 |
| `cv_p50 < 0,10` no s1, nos `-low` e no s5 | ✅ s1 0,074 / 0,062; `-low` ≤ 0,017; s5 0,008 / 0,006. O s3 também passa (0,004 / 0,002) |
| p99 do h2 cresce de forma monotônica de 1% → 5% nos `-low` | ✅ 84 → 261 → 446 ms, nas duas rodadas |
| s5 mostra a queda e a retomada, com `recovery_s` calculado | ✅ em todas as 120 repetições |
| As medianas das duas rodadas batem | ✅ (§1) |

---

## 3. s1-baseline — rede ideal (60 repetições, 300 RPC/s)

| Métrica | h2 | h3 | Δ (HL) [IC] | p (Holm) | δ |
|---|---|---|---|---|---|
| p50 | 2,70 ms | 3,37 ms | +0,69 [+0,65; +0,76] | 6·10⁻¹⁹ | +0,97 |
| p99 | 6,09 ms | 7,48 ms | +1,37 [+1,21; +1,50] | 2·10⁻¹⁵ | +0,86 |
| p99.9 | 8,89 ms | 10,50 ms | +1,34 [+0,87; +1,82] | 2·10⁻⁵ | +0,48 |
| handshake | 9,6 ms | 11,7 ms | +2,0 [+1,6; +2,3] | 8·10⁻¹¹ | +0,71 |
| CPU dos 3 serviços por 1000 RPC/s (`cpu_per_krps`) | 160 | 224 | +62 [+59; +65] | 6·10⁻¹⁹ | +0,97 |

CPU por contêiner (mediana, % de 1 vCPU), h2 / h3: gateway 14,7 / 19,9 (+35%), core 18,4 / 25,9 (+41%), ledger 15,2 / 21,5 (+41%), loadgen 10,6 / 15,9 (+50%).

**Leitura:**
- **Latência:** o h3 é ~25% mais lento no p50 e ~23% no p99. Os grupos quase não se sobrepõem (δ ≈ +0,9), mas em valor absoluto a diferença fica abaixo de 1,5 ms.
- **CPU:** o QUIC em espaço de usuário custa 40% a mais por RPC nos três serviços (razão das medianas; o HL é +62). A 300 RPC/s, o custo por RPC é dominado por custos fixos por pacote e por chamada de sistema, onde o QUIC pesa mais (a varredura da calibração mostra o prêmio do h3 caindo de 63% a 250 RPC/s para 16% a 750 RPC/s). **Hipótese:** é esse custo que deixa o h3 mais lento, porque cada RPC passa por 3 pilhas QUIC no lado servidor e 3 no lado cliente; os dados mostram a correlação, não a causa.
- **Handshake:** com RTT ≈ 0, mede CPU: três conexões abertas em série, e a inicialização do QUIC custa mais.
- **Para o TCC:** na rede ideal, as pilhas são equivalentes na prática (diferença < 1,5 ms), mas o custo de CPU do QUIC, multiplicado pelos três saltos, aparece na latência.

---

## 4. s2-loss-low — perda de pacotes (1%, 3%, 5% por direção em cada salto; 200 RPC/s; 60 repetições)

Os dois transportes atendem **100% da demanda** em todos os níveis. Fora duas repetições do h2 a 5% (§7), não há erros. A latência é de transporte, não de fila.

| Perda | Métrica | h2 | h3 | Δ (HL) [IC] | δ |
|---|---|---|---|---|---|
| 1% | p50 | 39,8 | 33,5 | −6,3 [−6,6; −6,2] | −1,00 |
| | p99 | 84,4 | 49,3 | −35,1 [−35,7; −34,5] | −1,00 |
| | p99.9 | 99,3 | 61,6 | −38,1 [−43,2; −36,8] | −1,00 |
| 3% | p50 | 62,2 | 33,6 | −28,6 [−28,8; −28,4] | −1,00 |
| | p99 | 261 | 61,6 | −200 [−206; −191] | −1,00 |
| | p99.9 | 311 | 74,1 | −237 [−241; −233] | −1,00 |
| 5% | p50 | 80,3 | 33,8 | −46,6 [−46,9; −46,0] | −1,00 |
| | p99 | 446 | 68,2 | −378 [−428; −313] | −1,00 |
| | p99.9 | 718 | 84,1 | −634 [−643; −626] | −1,00 |

Todos os p (Holm) < 10⁻¹⁹. Latências em ms.

**Leitura:**
- **O p50 do h3 não muda com a perda** (33,5 / 33,6 / 33,8 ms), igual ao RTT base de ponta a ponta mais o processamento.
- **No h2, toda a distribuição se desloca:** o p50 vai de 39,8 para 80,3 ms e o p99 de 84 para 446 ms. Cada retransmissão do TCP segura todos os streams da conexão, e uma RPC atravessa três conexões.
- **Em razão (h2/h3):** p50 de 1,19 / 1,85 / 2,38; p99 de 1,71 / 4,24 / 6,55.
- **Por salto:** a diferença no p50 (6,3 / 28,6 / 46,5 ms) dividida pelos 3 saltos dá ~2 / 10 / 16 ms de penalidade do h2 por conexão.
- **Engasgos de vazão (`degraded_s`):** só no h2, em 6 / 52 / 60 das 60 repetições a 1% / 3% / 5% (mediana de 6 s a 5%, máximo de 11). Nenhuma repetição do h3 tem engasgo.
- **CPU:** a 200 RPC/s, o custo por RPC é dominado por custos fixos e cai no h2 conforme a perda sobe (125 → 84), possivelmente porque as retransmissões agrupam as respostas (não medido). Não serve para comparar eficiência.

**Handshake (todos os s2-low):** mediana de 137 / 164 / 397 ms no h2 e 107 / 109 / 290 ms no h3. A 1%, a diferença é de −30,6 ms [−32,2; −29,3], os 3 RTTs de salto que a teoria prevê. A 3% e 5%, perdas no handshake de algum dos 3 saltos esperam o timeout inicial (casos acima de 500 ms: 15 e 24 repetições no h2, 5 e 23 no h3), e a diferença deixa de ser significativa a 5% (p = 0,48).

![CDF por cenário (pooled)](../results/pooled-payment-2026-10-16/latency_cdf.png)

*Pooled (60 repetições). Nos painéis `-low`, a curva do h3 é quase a mesma nos três níveis, enquanto a do h2 se desloca para a direita com a perda.*

---

## 5. s3-jitter — 20 ms ± 10 ms (normal) por lado em cada salto, 0% de perda, `rate 10gbit` (300 RPC/s; 60 repetições)

| Métrica | h2 | h3 | Δ (HL) [IC] | p (Holm) | δ |
|---|---|---|---|---|---|
| demanda atendida | 100% | 100% | — | 1,0 | 0 |
| p50 | 217 ms | 174 ms | −42,9 [−43,0; −42,8] | 4·10⁻²⁰ | −1,00 |
| p99 | 254 ms | 216 ms | −38,3 [−38,7; −37,8] | 4·10⁻²⁰ | −1,00 |
| p99.9 | 267 ms | 230 ms | −37,0 [−37,8; −36,1] | 4·10⁻²⁰ | −1,00 |
| handshake | 613 ms | 453 ms | −159 [−174; −145] | 4·10⁻²⁰ | −1,00 |
| CPU dos 3 serviços por 1000 RPC/s | 76,9 | 73,7 | −3,2 [−4,3; −2,3] | 1·10⁻⁴ | −0,43 |

**Leitura:**
- **Sem cauda do h3.** O p99 do h3 vai de 214 a 219 ms nas 60 repetições, e o do h2 de 251 a 256 ms (uma repetição com ruído de CPU chega a 282 ms). O maior p99 do h3 fica abaixo do menor do h2.
- **Sem episódios:** nenhuma repetição dos dois transportes tem segundos com vazão abaixo de 90% (`degraded_s = 0` em todas) ou janelas com o p99 do segundo acima de 2× o p50 da repetição.
- **O h3 é mais rápido em toda a distribuição**, não só na cauda: −43 ms no p50 e −38 ms no p99, ~14 ms por salto.
- **p50 acima do RTT base:** 217 e 174 ms contra 120 ms. A fila FIFO do netem (`rate 10gbit`) transforma o jitter em atraso maior.
- **Handshake:** 613 / 453 ms. A diferença (−159 ms) é de ~4 RTTs de base, mais que os 3 da teoria: sob jitter, o RTT efetivo de cada salto passa dos 40 ms de base.

**Sem cauda do h3 (hipótese, sem qlog nesta fase):** o quic-go v0.63 declara perdas falsas sob reordenação e não restaura a janela de congestionamento depois. Com 300 RPC/s de mensagens de ~40 B por conexão, a janela reduzida provavelmente continua acima do que a carga precisa, então essas perdas, se acontecem, não formam fila. Uma repetição com `QLOG=1` confirmaria (§8).

---

## 6. s5-outage — apagão de 5 s no egress da `rpcnet` do gateway (RTT de salto de 10 ms; 300 RPC/s; 60 repetições)

O apagão corta as respostas do gateway ao loadgen e os pedidos do gateway ao core. Nenhuma repetição tem `degraded_s` fora do apagão.

| Métrica | h2 | h3 | Δ (HL) [IC] | p (Holm) | δ |
|---|---|---|---|---|---|
| recovery_s | 2 s (59 × 2 s, 1 × 1 s) | 2 s (39 × 2 s, 4 × 1 s, 17 × 0 s) | 0 [0; 0] | 1·10⁻⁵ | −0,34 |
| p50 | 33,2 ms | 34,2 ms | +1,07 [+1,02; +1,11] | 7·10⁻¹⁹ | +0,97 |
| p99 (backlog do apagão) | 2,18 s | 2,18 s | −0,012 s [−0,033; +0,007] | 0,21 | −0,13 |
| erros | 7,46% | 7,22% | −0,74 pp [−1,08; −0,24] | 4·10⁻⁸ | −0,62 |
| handshake | 136,7 ms | 106,2 ms | −30,2 [−31,0; −29,4] | 4·10⁻²⁰ | −1,00 |
| CPU dos 3 serviços por 1000 RPC/s | 171 | 245 | +73 [+70; +77] | 7·10⁻¹⁹ | +0,97 |

**Leitura:**
- **Recuperação:** a primeira resposta depois do fim do apagão chega em 1,8–2,3 s no h2 (44 de 60 repetições) ou em ~2,7 s (16). No h3, chega em 1,8–2,5 s (41) ou em 0,55–0,99 s (19). Em resolução de segundos, os dois voltam em 2 s; o h3 tem uma moda rápida em ~30% das repetições, e o h2 não tem. Medido pela primeira resposta, o h3 volta 0,33 s antes (HL; δ −0,42).
- **Por que 2 s (hipótese):** o apagão no gateway corta duas conexões, cada uma com seus temporizadores (RTO no TCP, PTO no QUIC) dobrando a cada tentativa sem resposta. A cadeia só volta quando a mais atrasada dispara. A fase desses disparos em relação ao fim do apagão decide se a volta cai em ~1 s ou em ~2 s. Os arquivos da rodada não permitem dizer qual das duas conexões atrasa a volta.
- **Erros:** toda RPC enviada até 2 s antes da primeira resposta estoura o prazo; isso explica a contagem de cada repetição (correlação de 0,91 no h2 e 0,98 no h3 com o instante da primeira resposta). Com 300 RPC/s e prazo de 2 s, os workers se liberam a tempo de enviar quase todos os slots do apagão, então a taxa de erro (7,2–7,5%) reflete o tempo de volta.
- **Backlog:** o p99 é o prazo de 2 s mais o atraso dos workers e empata. O pico logo depois da volta chega a ~3× a taxa (~900–1000 RPC/s, conforme o alinhamento dos bins de 1 s).
- **Fora do apagão:** o p50 do h3 é 1,1 ms maior, o mesmo efeito de CPU do s1.
- **Achado de arquitetura (hipótese):** no h2, o core processa ~760 pedidos por repetição que terminam com erro no loadgen; no h3, ~40 (mediana de `events_published` do core − RPCs `ok` de todas as fases − 1 do probe). Os dados não dizem quando o core os processou: parte pode ter sido processada antes do prazo, com a resposta perdida no apagão. A explicação provável para a diferença: o TCP entrega ao core os bytes de streams já cancelados que ficaram no buffer durante o apagão, e o `grpc-timeout` reinicia o prazo em cada salto; o QUIC descarta os dados dos streams cancelados. É uma diferença de comportamento sob falha: trabalho feito para pagamentos que o cliente já deu como falhos. Medir isso diretamente é o próximo passo (§8).
- **Handshake:** −30,2 ms, 3 RTTs de salto, como no s2.
- **Origem:** a primeira resposta, o pico, a correlação e o trabalho órfão vêm de scripts sobre o `requests.csv`, o `events.csv` e o `svc-core.json`; não estão nos CSVs do `analyze`.

![s5 vazão](../results/pooled-payment-2026-10-16/s5_throughput.png)

*s5 (60 repetições por transporte): todas as linhas zeram no apagão e voltam com o pico de backlog. As do h3 que sobem logo no primeiro segundo depois do fim são a moda rápida.*

---

## 7. Validade — o que pode enganar

| Problema | Onde | Efeito | Tratamento |
|---|---|---|---|
| Ruído leve do host | rodada B, a partir do s1 rep 12 (~11:50 UTC) até o s3 | CPU por RPC de todos os contêineres 4–17% acima de A, inclusive Postgres e loadgen (s5: −1% a +6%); p50 do s1 +6% (h2) e +10% (h3) | os dois transportes alternam, e o HL de CPU não muda (61,9 contra 61,7); nenhuma janela descartada |
| Ruído pontual de CPU (`cpu_outlier`) | 8 repetições: na rodada A, 3 no s1, 2 no s5 e 1 no s3 (h3); na B, 1 no s3 (h2) e 1 no s2-loss-5-low | CPU 1,2–2,5× a do grupo por ~1 min; no s1, p50 até +53% | `--exclude-degraded` tira as 5 do s1 e do s5: as medianas de p50 e p99 mudam ≤ 0,3% e os HL de p50 e p99 ≤ 1,5% (o HL do erro no s5 vai de −0,74 para −0,58 pp); nenhuma conclusão muda. O texto usa a pooled completa |
| Erros por prazo sob perda | 2 repetições do h2 no s2-loss-5-low (rodada A) | 2,3% e 2,4% de `deadline_exceeded`; demanda atendida 97,6% | retransmissões com backoff passando do prazo de 2 s; ficam na análise (`p*_all`) |
| `late_starts` | todos | 4,7–15,7% dos slots (s5: 12–22%); o h2 tem um pouco mais que o h3 no s2-low (~8% contra ~6%) e no s5 (17% contra 14,5%), e menos no s3 (9% contra 10%) | latência da VM para acordar o goroutine (§Calibração); já entra na latência, medida desde o horário planejado. A diferença entre os transportes, pequena, não está explicada (a spec §9 supunha que seria igual) |
| CPU dos contêineres de dados | s1 | Postgres, RabbitMQ e Risk gastam 12–21% mais CPU com h3, para o mesmo trabalho | não explicado (hipótese: processamento de rede contabilizado no contêiner que está rodando); fora do caminho medido |
| `recovery_s` em bins de 1 s | s5 | primeira resposta de 1,8 a 2,7 s vira 2 s | a primeira resposta em resolução de ms entra na leitura (§6) |
| Shutdown lento | 24 repetições do h3 no s2 e no s3 | `stopped error="context deadline exceeded"` nos logs, depois da medição | sem efeito: os `svc-*.json` foram gravados e os contadores batem |

---

## 8. Próximos passos

1. **Consistência sob timeout:** contar no ledger os pagamentos liquidados e cruzar com as RPCs `ok`, para medir diretamente os pagamentos processados que o cliente deu como falhos (o trabalho órfão do s5, §6).
2. **Cenário 4** (migração de IP do cliente): fase própria (spec §13).
3. **Onde está a falha:** perda ou apagão em um salto só (borda, gateway → core, core → ledger), para mapear o impacto por padrão de comunicação.
4. **s3:** uma repetição com `QLOG=1` para confirmar as perdas falsas do quic-go e que elas não reduzem a vazão.
5. **s5:** um log por RPC no gateway e no core, ou uma captura de pacotes, numa repetição, para saber qual conexão atrasa a volta.
6. **Texto do TCC:** usar mediana, HL com IC e δ; o p de Holm só como confirmação. Gráficos: o CDF e o `s5_throughput` da pooled. No s5, descrever a distribuição (2 s nos dois, com a moda rápida do h3), não só a média.

---

### Apêndice — gráficos

Os PNGs ficam em `results/<pasta>/analysis/` (ou na pasta da pooled); essas pastas não são versionadas.

| Conjunto | CDF | Boxplot p99 | Vazão s5 |
|---|---|---|---|
| pooled A + B (60 reps) | [latency_cdf](../results/pooled-payment-2026-10-16/latency_cdf.png) | [p99_boxplot](../results/pooled-payment-2026-10-16/p99_boxplot.png) | [s5_throughput](../results/pooled-payment-2026-10-16/s5_throughput.png) |
| rodada A (30 reps) | [latency_cdf](../results/2026-10-15-23-55-55/analysis/latency_cdf.png) | [p99_boxplot](../results/2026-10-15-23-55-55/analysis/p99_boxplot.png) | [s5_throughput](../results/2026-10-15-23-55-55/analysis/s5_throughput.png) |
| rodada B (30 reps) | [latency_cdf](../results/2026-10-16-08-21-08/analysis/latency_cdf.png) | [p99_boxplot](../results/2026-10-16-08-21-08/analysis/p99_boxplot.png) | [s5_throughput](../results/2026-10-16-08-21-08/analysis/s5_throughput.png) |

---

## Calibração

### Resultado

| Parâmetro | Valor | Base |
|---|---|---|
| Capacidade de saturação (C) | 1000 RPC/s | maior taxa da varredura em que o h3 atende ≥ 99% da demanda; a 1500, nenhum dos dois atende (§9) |
| Taxa alta (s1, s3, s5) | **300 RPC/s** | 0,4 × C = 400 dá p50 de ~1,45× no h3 num ambiente limpo, no limite da saúde; 300 dá ~1,15× |
| Taxa baixa (`s2-loss-{1,3,5}-low`) | **200 RPC/s** | o h2 a 5% de perda atende 100%, sem erros (3 de 3 repetições) |
| Prazo por RPC | 2 s (sem mudança) | 0 erros no `s2-loss-5-low` |
| `s2-loss-{1,3,5}` (capacidade sob perda) | **fora da matriz** | regra de corte da §9: taxa alta ≤ 2 × taxa baixa (300 ≤ 400) |
| Concorrência, janela e aquecimento | 500 workers, 60 s, 10 s (sem mudança) | — |
| Aptidão do ambiente | `make fitness`: `cpu_per_krps` do h2 ≤ 200 a 250 RPC/s | antes de toda rodada longa |

### Varredura (2026-10-13, s1, 3 repetições por taxa e transporte, ambiente limpo)

Medianas entre as repetições. CPU em % de 1 vCPU (cada contêiner tem um núcleo; "núcleo 5" soma Postgres, RabbitMQ e Risk). Rodadas: `results/calib-{250,500,750,1000,1500}-2026-10-13-*`.

| RPC/s | | demanda atendida (mín.) | p50 | gateway | core | ledger | loadgen | núcleo 5 | `cpu_per_krps` |
|---|---|---|---|---|---|---|---|---|---|
| 250 | h2 | 1,00 | 2,5 ms | 10% | 13% | 11% | 8% | 20% | 135 |
| | h3 | 1,00 | 3,2 ms | 17% | 21% | 18% | 13% | 25% | 220 |
| 500 | h2 | 1,00 | 3,8 ms | 33% | 39% | 38% | 26% | 56% | 220 |
| | h3 | 1,00 | 5,6 ms | 47% | 55% | 55% | 41% | 68% | 314 |
| 750 | h2 | 1,00 | 5,2 ms | 52% | 59% | 60% | 41% | 81% | 228 |
| | h3 | 1,00 | 6,6 ms | 62% | 69% | 68% | 53% | 85% | 265 |
| 1000 | h2 | 1,00 | 7,4 ms | 66% | 72% | 71% | 49% | 90% | 209 |
| | h3 | 1,00 | 8,6 ms | 69% | 76% | 74% | 59% | 90% | 219 |
| 1500 | h2 | 0,88 | 3,3 s (fila) | 78% | 83% | 81% | 44% | 93% | 174 |
| | h3 | 0,94 | 2,5 s (fila) | 77% | 83% | 81% | 47% | 95% | 169 |

- **Primeiro gargalo:** o núcleo 5, onde Postgres, RabbitMQ e Risk dividem uma vCPU. No h3, a mediana chega a 68% a 500 RPC/s (máximo de 72% numa repetição); no h2, passa de 70% a 750 RPC/s.
- **Custo do QUIC:** o `cpu_per_krps` do h3 fica 63% acima do h2 a 250 RPC/s, 43% a 500 e 16% a 750. A diferença encolhe com a carga, conforme os custos fixos por RPC pesam menos.
- **Estabilidade:** as repetições de cada taxa variam pouco (`cpu_per_krps` ±5%). Só uma ficou marcada como `cpu_outlier`: a 250/h2 rep-1, primeira da varredura, com +24%.

### Conferências (2026-10-14)

| Rodada | Transporte | Demanda | Erros | p50 | p99 | Leitura |
|---|---|---|---|---|---|---|
| `check-low-2026-10-14-18-20-59` (`s2-loss-5-low`, 200 RPC/s) | h2 | 1,00 (3/3) | 0 | 83–94 ms | 466–932 ms | taxa baixa confirmada |
| | h3 | 1,00 (3/3) | 0 | 37 ms | 72–73 ms | — |
| `check-high-2026-10-14-18-09-03` (s1, 400 RPC/s) | h2 / h3 | 2 colapsos em 5 repetições | até 99% | segundos | — | **inválida:** host degradado (mesmo trabalho com 1,6–1,9× a CPU de 13/10) |
| `check-high-2026-10-14-20-02-28` (s1, 400 RPC/s) | h2 | 1,00 | 0 | 5,4–6,2 ms (2,1–2,5×) | 12–85 ms | saúde falha no p50; `cpu_per_krps` 294–351, ainda ~1,4× o de 13/10 |
| | h3 | 1,00 | 0 | 6,2–7,1 ms (1,9–2,2×) | 15–26 ms | idem; núcleo 5 em 62–71% |
| **`check-high-2026-10-14-20-28-21` (s1, 300 RPC/s), depois do `make fitness`** | h2 | 1,00 | 0 | 2,6–2,7 ms (~1,07×) | 5,7–6,1 ms | **saudável:** núcleo 5 ≤ 27%, core ≤ 18%; `cpu_per_krps` 153–162 |
| | h3 | 1,00 | 0 | 3,33–3,37 ms (~1,05×) | 6,7–8,0 ms | **saudável:** núcleo 5 ≤ 31%, core ≤ 26%; `cpu_per_krps` 218–225; sem `degraded_s` nem `cpu_outlier` |

- **Por que 300 e não 400:** a 400 RPC/s, a checagem de saúde (p50 ≤ 1,5×) já fica no limite num dia limpo (~1,45× no h3, interpolado) e falhou nas duas conferências de 14/10, em que a VM rendia menos. A 300, a interpolação da varredura limpa dá, no h3, p50 de ~3,7 ms (~1,15×), núcleo 5 em ~34% e core em ~28%. A margem absorve a variação de ambiente que vimos (1,3–2× no custo de CPU entre dias).
- **Conferência que fecha a P4 (feita):** `make fitness`, seguido de s1 a 300 RPC/s com 3 repetições (§12, P4). Os números ficaram até um pouco melhores que a interpolação (p50 do h3 a 1,05× da base).

### Diagnósticos do ambiente (2026-10-12)

| Rodada | Achado |
|---|---|
| `quick-calib-*`, `diag-250-*` | p50 de 8 ms a 250 RPC/s e um colapso do h3 (66% da demanda): 20 s em que a CPU de **todos** os contêineres (inclusive Postgres e loadgen) subiu 3–5× para a mesma carga |
| `diag2-250-*` + `vmstat` | as 3 primeiras repetições da rodada (~5 min) custaram 2,3× mais CPU, nos dois transportes, e depois tudo voltou ao normal; o `st` ficou em 0 (o Hyper-V não reporta steal ao Linux) |
| `diag3-250-*` (vCPU reservada no Hyper-V, host parado) | as 8 repetições saíram limpas e mais baratas do que as "limpas" do diag2 (soma dos contêineres: 62–113% contra 109–161%); p50 de 2,6–3,6 ms |
| `late_starts` | 4–15% dos slots a 250 RPC/s com o loadgen a menos de 25% de CPU, com 1 ou 2 vCPUs: a VM demora a acordar o goroutine que dispara os pedidos entre slots. Ele é reportado, mas não decide a calibração (§9) |
| `rpclog` | o campo `protocol` é o protocolo RPC (`grpc`); a versão HTTP servida foi para o campo `http` das linhas de cliente |

**Consequências para as rodadas:**
- a vCPU fica reservada no Hyper-V e o host parado;
- `make fitness` roda antes de toda rodada longa;
- o `--exclude-degraded` também descarta repetições com `cpu_outlier` nos cenários sem falha injetada.
