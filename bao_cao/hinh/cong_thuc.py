"""Công thức của báo cáo, vẽ thành ảnh bằng mathtext của matplotlib (phông STIX giống Times)."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams["mathtext.fontset"] = "stix"
plt.rcParams["font.family"] = "STIXGeneral"

CT = {
    "pref": r"$p_{ui}=\mathbf{1}\,[\,r_{ui}>0\,],\qquad c_{ui}=1+\alpha\, r_{ui}$",
    "als": r"$\min_{X,\,Y}\ \sum_{u,i} c_{ui}\left(p_{ui}-x_u^{\top}y_i\right)^2+\lambda\left(\sum_u n_u\Vert x_u\Vert^2+\sum_i n_i\Vert y_i\Vert^2\right)$",
    "decay": r"$s(u,i)=\sum_{e\,\in\,E(u,i)} w_e\, e^{-\lambda_d\,\Delta d_e},\qquad w_{view}=1,\ w_{cart}=3,\ w_{buy}=5,\ \lambda_d=0.01$",
    "gop": r"$e^{-\lambda_d (c-d)}\cdot e^{-\lambda_d (e-c)}=e^{-\lambda_d (e-d)}$",
    "pr": r"$P@K=\dfrac{|R_K(u)\cap T(u)|}{K},\qquad R@K=\dfrac{|R_K(u)\cap T(u)|}{|T(u)|}$",
    "ndcg": r"$DCG@K=\sum_{j=1}^{K}\dfrac{rel_j}{\log_2(j+1)},\qquad NDCG@K=\dfrac{DCG@K}{IDCG@K}$",
    "map": r"$AP@K=\dfrac{1}{\min(|T(u)|,K)}\ \sum_{j=1}^{K} P@j\cdot rel_j$",
    "covnov": r"$Coverage=\dfrac{|\bigcup_u R_K(u)|}{|I_{train}|},\qquad Novelty=\dfrac{1}{\sum_u |R_K(u)|}\sum_u\sum_{i\in R_K(u)} -\log_2\dfrac{n_i}{N}$",
    "uplift": r"$uplift=\dfrac{NDCG_{ALS}-NDCG_{B1}}{NDCG_{B1}}\ \geq\ 30\,\%\quad \mathrm{and}\quad Coverage\ \geq\ 10\,\%$",
    "rerank": r"$score(u,i)=\widetilde{r}_{ui}+\beta\cdot\widetilde{nov}_i,\qquad \widetilde{x}=\dfrac{x-\min x}{\max x-\min x}\ \mathrm{within\ the\ candidates\ of}\ u$",
    "trend": r"$trend(i,w)=\dfrac{views(i,w)+3\cdot carts(i,w)}{\overline{n}_{24h}(i)+10}$",
    "z": r"$z=\dfrac{\hat p_B-\hat p_A}{\sqrt{\hat p\,(1-\hat p)\left(\dfrac{1}{n_A}+\dfrac{1}{n_B}\right)}},\qquad \hat p=\dfrac{x_A+x_B}{n_A+n_B},\qquad p=\mathrm{erfc}\left(\dfrac{|z|}{\sqrt{2}}\right)$",
    "n": r"$n=\dfrac{\left(z_{\alpha/2}+z_{\beta}\right)^2\left(p_1(1-p_1)+p_2(1-p_2)\right)}{(p_2-p_1)^2}$",
    "click": r"$P(click)=v^{-\eta}\cdot P(rel\,|\,u,i),\qquad P(rel\,|\,u,i)=0.40\ \mathrm{if}\ i\in T(u),\ \mathrm{else}\ 0.03,\qquad \eta=0.7$",
    "cos": r"$sim(i,j)=\dfrac{n(i,j)}{\sqrt{n(i)\,n(j)}}$",
    "tam_gia": r"$[\,0.5\cdot P_{10}(price_u),\ 1.5\cdot P_{90}(price_u)\,]$",
}

for ten, bt in CT.items():
    fig = plt.figure(figsize=(0.01, 0.01))
    fig.text(0, 0, bt, fontsize=15)
    fig.savefig("eq_%s.png" % ten, dpi=220, bbox_inches="tight", pad_inches=0.04, facecolor="white")
    plt.close(fig)
print(len(CT), "công thức")
