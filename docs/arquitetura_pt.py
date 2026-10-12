"""Figura de arquitetura do artigo (em português).

Gerar com:
    uv run python docs/arquitetura_pt.py
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

OUT = Path(__file__).with_name("arquitetura_pt.png")
plt.rcParams["font.family"] = "DejaVu Sans"

fig, ax = plt.subplots(figsize=(16, 9.2), dpi=150)
ax.set_xlim(0, 16)
ax.set_ylim(0, 9.2)
ax.axis("off")


def caixa(
    x,
    y,
    w,
    h,
    titulo,
    sub="",
    fundo="#F8FAFC",
    borda="#64748B",
    cor="#0F172A",
    tam=13,
    estilo="round,pad=0.02,rounding_size=0.12",
    tracejada=False,
    lw=1.6,
):
    ax.add_patch(
        FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle=estilo,
            facecolor=fundo,
            edgecolor=borda,
            linewidth=lw,
            linestyle="--" if tracejada else "-",
        )
    )
    if sub:
        ax.text(
            x + w / 2,
            y + h * 0.64,
            titulo,
            ha="center",
            va="center",
            fontsize=tam,
            fontweight="bold",
            color=cor,
        )
        ax.text(
            x + w / 2,
            y + h * 0.30,
            sub,
            ha="center",
            va="center",
            fontsize=tam - 3.5,
            color=cor,
            linespacing=1.3,
        )
    else:
        ax.text(
            x + w / 2,
            y + h / 2,
            titulo,
            ha="center",
            va="center",
            fontsize=tam,
            fontweight="bold",
            color=cor,
        )


def pilula(x, y, texto, largura=2.2):
    ax.add_patch(
        FancyBboxPatch(
            (x - largura / 2, y - 0.2),
            largura,
            0.4,
            boxstyle="round,pad=0.02,rounding_size=0.2",
            facecolor="#FFFBEB",
            edgecolor="#B45309",
            linewidth=1.4,
        )
    )
    ax.text(x, y, texto, ha="center", va="center", fontsize=8.8, fontweight="bold", color="#78350F")


def seta(x1, y1, x2, y2, cor="#334155", tracejada=False, lw=1.6):
    ax.add_patch(
        FancyArrowPatch(
            (x1, y1),
            (x2, y2),
            arrowstyle="-|>",
            mutation_scale=14,
            color=cor,
            linewidth=lw,
            linestyle="--" if tracejada else "-",
        )
    )


# Faixas de orquestração e transformação
caixa(
    0.6,
    8.25,
    14.8,
    0.6,
    "Dagster: orquestração orientada a ativos, "
    "execução dos portões e interrupção do fluxo em caso de falha",
    fundo="#EEF2FF",
    borda="#4F46E5",
    cor="#1E1B4B",
    tam=12,
)
caixa(
    0.6,
    7.45,
    14.8,
    0.6,
    "dbt sobre DuckDB: uma etapa por camada, "
    "gravada em Delta Lake por um plugin próprio (delta-rs)",
    fundo="#ECFDF5",
    borda="#0F766E",
    cor="#134E4A",
    tam=12,
)
seta(8, 7.45, 8, 7.05)

# Contêiner do armazenamento de objetos (SeaweedFS)
caixa(0.4, 3.75, 15.2, 3.25, "", fundo="none", borda="#94A3B8", tracejada=True, lw=1.4)
ax.text(
    0.65,
    6.78,
    "SeaweedFS: armazenamento de objetos compatível com S3",
    fontsize=10.5,
    fontweight="bold",
    color="#334155",
    va="center",
)

y, h, w = 4.2, 1.4, 2.7
cx = {"landing": 2.05, "bronze": 6.65, "silver": 10.25, "gold": 13.85}
caixa(cx["landing"] - w / 2, y, w, h, "Landing", "CSV e JSON brutos\nfronteira externa")
caixa(
    cx["bronze"] - w / 2,
    y,
    w,
    h,
    "Bronze",
    "tabela Delta\ntipos padronizados",
    fundo="#FFEDD5",
    borda="#C2410C",
    cor="#7C2D12",
)
caixa(
    cx["silver"] - w / 2,
    y,
    w,
    h,
    "Silver",
    "tabela Delta\nlimpa e enriquecida",
    fundo="#E2E8F0",
    borda="#475569",
    cor="#0F172A",
)
caixa(
    cx["gold"] - w / 2,
    y,
    w,
    h,
    "Gold",
    "tabela Delta\nmétricas de negócio",
    fundo="#FEF9C3",
    borda="#A16207",
    cor="#713F12",
)

# Portão de pré-voo no caminho entre Landing e Bronze
meio = y + h / 2
gx, gw = 4.35, 1.5
ax.add_patch(
    FancyBboxPatch(
        (gx - gw / 2, meio - 0.33),
        gw,
        0.66,
        boxstyle="round,pad=0.02,rounding_size=0.2",
        facecolor="#FEF3C7",
        edgecolor="#B45309",
        linewidth=1.5,
    )
)
ax.text(
    gx,
    meio,
    "portão de\npré-voo",
    ha="center",
    va="center",
    fontsize=9.5,
    fontweight="bold",
    color="#78350F",
    linespacing=1.15,
)
seta(cx["landing"] + w / 2, meio, gx - gw / 2, meio)
seta(gx + gw / 2, meio, cx["bronze"] - w / 2, meio)
seta(cx["bronze"] + w / 2, meio, cx["silver"] - w / 2, meio)
seta(cx["silver"] + w / 2, meio, cx["gold"] - w / 2, meio)

# Verificações pós-escrita acima de cada camada curada
for nome, rotulo in (
    ("bronze", "(bloqueante)"),
    ("silver", "(bloqueante)"),
    ("gold", "(não bloqueante)"),
):
    x0 = cx[nome]
    ax.add_patch(
        FancyBboxPatch(
            (x0 - 1.3, y + h + 0.22),
            2.6,
            0.62,
            boxstyle="round,pad=0.02,rounding_size=0.18",
            facecolor="#FFFBEB",
            edgecolor="#B45309",
            linewidth=1.3,
        )
    )
    ax.text(
        x0,
        y + h + 0.53,
        "verificação pós-escrita\n" + rotulo,
        ha="center",
        va="center",
        fontsize=8.8,
        fontweight="bold",
        color="#78350F",
        linespacing=1.15,
    )
    ax.plot([x0, x0], [y + h, y + h + 0.22], color="#B45309", linewidth=1.2, linestyle=":")

# Quarentena
qx, qy, qw, qh = 5.6, 2.2, 4.8, 0.95
caixa(
    qx,
    qy,
    qw,
    qh,
    "Quarentena",
    "s3://<bucket>/quarantine/dt=.../reason=...",
    fundo="#FEF2F2",
    borda="#DC2626",
    cor="#7F1D1D",
)
seta(gx, meio - 0.33, qx + 0.6, qy + qh, cor="#DC2626", tracejada=True)
seta(cx["bronze"], y, qx + 2.0, qy + qh, cor="#DC2626", tracejada=True)
seta(cx["silver"], y, qx + 3.6, qy + qh, cor="#DC2626", tracejada=True)
# Gold: a falha copia o lote, mas não interrompe o fluxo
seta(cx["gold"] - 0.7, y, qx + qw, qy + qh * 0.55, cor="#F87171", tracejada=True, lw=1.3)
ax.text(
    12.1,
    3.05,
    "cópia, sem\ninterrupção",
    fontsize=8.5,
    color="#B91C1C",
    ha="left",
    va="center",
    linespacing=1.2,
)
ax.text(
    qx - 0.2,
    qy + qh / 2,
    "lote reprovado:\ncópia para auditoria e\ninterrupção do fluxo",
    fontsize=9,
    color="#B91C1C",
    va="center",
    ha="right",
    linespacing=1.3,
)

# Consumidores
for x, t, s in [
    (3.0, "Painel Streamlit", "lê a partição Gold mais recente"),
    (6.75, "dbt Docs", "catálogo e linhagem dos modelos"),
    (10.5, "Relatórios do GX", "resultado de cada validação"),
]:
    caixa(x, 0.3, 3.1, 1.05, t, s, fundo="#EFF6FF", borda="#2563EB", cor="#1E3A8A", tam=11.5)
ax.plot([cx["gold"], cx["gold"], 4.55], [y, 1.75, 1.75], color="#334155", linewidth=1.4)
for x in (4.55, 8.3, 12.05):
    seta(x, 1.75, x, 1.35)

fig.savefig(OUT, bbox_inches="tight", facecolor="white")
print(f"figura salva em {OUT}")
