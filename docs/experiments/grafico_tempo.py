"""Figura 2 do artigo: tempo de execução por cenário, a partir de results.json.

Gerar com:
    uv run --with matplotlib python docs/experiments/grafico_tempo.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

AQUI = Path(__file__).parent
OUT = AQUI / "tempo_execucao.png"
e1 = json.loads((AQUI / "results.json").read_text(encoding="utf-8"))["e1_overhead"]

plt.rcParams["font.family"] = "DejaVu Sans"
fig, ax = plt.subplots(figsize=(7.5, 4.2), dpi=200)

cenarios = [
    ("A: sem portões", e1["a_seconds"], "#94A3B8"),
    ("B: com portões", e1["b_seconds"], "#B45309"),
]
for i, (_rotulo, tempos, cor) in enumerate(cenarios):
    media = sum(tempos) / len(tempos)
    ax.barh(i, media, color=cor, height=0.55, zorder=2)
    ax.scatter(
        tempos,
        [i] * len(tempos),
        color="#0F172A",
        s=14,
        zorder=3,
        label="execução individual" if i == 0 else None,
    )
    ax.text(
        max(tempos) + 0.6,
        i,
        f"{media:.1f} s".replace(".", ","),
        va="center",
        fontsize=10,
        fontweight="bold",
        color="#0F172A",
    )

ax.set_yticks(range(len(cenarios)), [c[0] for c in cenarios], fontsize=10)
ax.invert_yaxis()
ax.set_xlabel("tempo de execução do job (s)", fontsize=10)
ax.set_xlim(0, max(e1["b_seconds"]) * 1.18)
ax.grid(axis="x", color="#E2E8F0", zorder=0)
for lado in ("top", "right", "left"):
    ax.spines[lado].set_visible(False)
ax.tick_params(axis="y", length=0)
ax.legend(loc="upper right", frameon=False, fontsize=9)
ax.set_title(
    f"Conjunto limpo, média de {len(e1['a_seconds'])} execuções por cenário",
    fontsize=10,
    loc="left",
    color="#334155",
)

fig.tight_layout()
fig.savefig(OUT, facecolor="white")
print(f"figura salva em {OUT}")
