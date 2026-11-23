[val] quy mô: {"phien_ban_features": 3, "phien_ban_silver": 10, "ngay_cat": "2015-08-19", "ngay_cuoi": "2015-09-18", "so_ngay_val": 15, "so_user_ca_thang": 9467, "so_user_val": 6505, "so_user_kiem_tra": 4215, "so_user_ca_hai_ky": 1253, "so_item_train": 126230}
[val] rank=50 reg=0.01 alpha=40.0 val=0.07709 kiem_tra=0.05812 ca_thang=0.06684 (524.6s)
[val] rank=50 reg=0.1 alpha=40.0 val=0.07631 kiem_tra=0.05741 ca_thang=0.06620 (503.5s)
[val] rank=50 reg=1.0 alpha=40.0 val=0.07886 kiem_tra=0.05790 ca_thang=0.06802 (452.8s)
[val] beta=0.0 val=0.07886 kiem_tra=0.05790 coverage=0.0283
[val] beta=0.25 val=0.08553 kiem_tra=0.06296 coverage=0.0324
[val] beta=0.5 val=0.09193 kiem_tra=0.06741 coverage=0.0385
[val] beta=1.0 val=0.10122 kiem_tra=0.07417 coverage=0.0527
[val] beta=1.5 val=0.08185 kiem_tra=0.05761 coverage=0.0597
[val] beta=2.0 val=0.06298 kiem_tra=0.04389 coverage=0.0581

## Tuning trên kỳ validation riêng — features v3, validation 15 ngày đầu của tháng test, 3 tổ hợp

| Kỳ | User |
|---|---|
| Cả tháng test | 9,467 |
| Validation (15 ngày đầu) | 6,505 |
| Kiểm tra (phần còn lại) | 4,215 |
| Có mặt ở cả hai kỳ | 1,253 |

| rank | regParam | alpha | NDCG@10 validation | NDCG@10 kiểm tra | NDCG@10 cả tháng |
|---|---|---|---|---|---|
| 50 | 1.0 | 40.0 | 0.0789 **(chọn)** | 0.0579 | 0.0680 |
| 50 | 0.01 | 40.0 | 0.0771 | 0.0581 | 0.0668 |
| 50 | 0.1 | 40.0 | 0.0763 | 0.0574 | 0.0662 |

| Trên kỳ kiểm tra | NDCG@10 | So với baseline toàn sàn | So với baseline danh mục |
|---|---|---|---|
| Baseline toàn sàn | 0.0049 | — | — |
| Baseline danh mục | 0.0413 | — | — |
| ALS chọn trên validation (rank 50, reg 1.0, alpha 40.0) | 0.0579 | +1086% | +40% |
| ALS tốt nhất nếu chọn trên chính kỳ kiểm tra (rank 50, reg 0.01, alpha 40.0) | 0.0581 | +1091% | +41% |
| Xếp lại β = 1.0 (chọn trên validation) | 0.0742 | +1420% | +80% |

| β | NDCG@10 validation | NDCG@10 kiểm tra | Coverage kiểm tra | Số item |
|---|---|---|---|---|
| 0.0 | 0.0789 | 0.0579 | 2.83% | 3,568 |
| 0.25 | 0.0855 | 0.0630 | 3.24% | 4,091 |
| 0.5 | 0.0919 | 0.0674 | 3.85% | 4,856 |
| 1.0 | 0.1012 **(chọn)** | 0.0742 | 5.27% | 6,649 |
| 1.5 | 0.0819 | 0.0576 | 5.97% | 7,536 |
| 2.0 | 0.0630 | 0.0439 | 5.81% | 7,336 |

```json
{
 "quy_mo": {
  "phien_ban_features": 3,
  "phien_ban_silver": 10,
  "ngay_cat": "2015-08-19",
  "ngay_cuoi": "2015-09-18",
  "so_ngay_val": 15,
  "so_user_ca_thang": 9467,
  "so_user_val": 6505,
  "so_user_kiem_tra": 4215,
  "so_user_ca_hai_ky": 1253,
  "so_item_train": 126230,
  "so_to_hop": 3
 },
 "chon_tren_validation": {
  "rank": 50,
  "reg_param": 1.0,
  "alpha": 40.0,
  "ndcg_val": 0.0788621142300091,
  "ndcg_kiem_tra": 0.057901535215392604,
  "ndcg_ca_thang": 0.06801500098427876,
  "recall_kiem_tra": 0.08065770309863081,
  "so_giay": 452.8
 },
 "tot_nhat_tren_kiem_tra": {
  "rank": 50,
  "reg_param": 0.01,
  "alpha": 40.0,
  "ndcg_val": 0.07708678217716598,
  "ndcg_kiem_tra": 0.058121390307330184,
  "ndcg_ca_thang": 0.06684048079506107,
  "recall_kiem_tra": 0.08070917962975956,
  "so_giay": 524.6
 },
 "tot_nhat_tren_ca_thang": {
  "rank": 50,
  "reg_param": 1.0,
  "alpha": 40.0,
  "ndcg_val": 0.0788621142300091,
  "ndcg_kiem_tra": 0.057901535215392604,
  "ndcg_ca_thang": 0.06801500098427876,
  "recall_kiem_tra": 0.08065770309863081,
  "so_giay": 452.8
 },
 "baseline_kiem_tra": {
  "toan_san": 0.0048813718311768685,
  "danh_muc": 0.041285693665751494,
  "coverage_toan_san": 7.922047056959519e-05,
  "coverage_danh_muc": 0.044339697377802426
 },
 "beta_chon_tren_validation": {
  "beta": 1.0,
  "ndcg_val": 0.10122244999694796,
  "ndcg_kiem_tra": 0.07417297849632949,
  "coverage_kiem_tra": 0.052673690881723835,
  "so_item_kiem_tra": 6649
 },
 "beta_tot_nhat_tren_kiem_tra": {
  "beta": 1.0,
  "ndcg_val": 0.10122244999694796,
  "ndcg_kiem_tra": 0.07417297849632949,
  "coverage_kiem_tra": 0.052673690881723835,
  "so_item_kiem_tra": 6649
 },
 "cac_beta": [
  {
   "beta": 0.0,
   "ndcg_val": 0.07886211423000908,
   "ndcg_kiem_tra": 0.057901535215392604,
   "coverage_kiem_tra": 0.028265863899231563,
   "so_item_kiem_tra": 3568
  },
  {
   "beta": 0.25,
   "ndcg_val": 0.0855278206193637,
   "ndcg_kiem_tra": 0.06295861776595231,
   "coverage_kiem_tra": 0.03240909451002139,
   "so_item_kiem_tra": 4091
  },
  {
   "beta": 0.5,
   "ndcg_val": 0.09192710878925597,
   "ndcg_kiem_tra": 0.06740700162357915,
   "coverage_kiem_tra": 0.03846946050859542,
   "so_item_kiem_tra": 4856
  },
  {
   "beta": 1.0,
   "ndcg_val": 0.10122244999694796,
   "ndcg_kiem_tra": 0.07417297849632949,
   "coverage_kiem_tra": 0.052673690881723835,
   "so_item_kiem_tra": 6649
  },
  {
   "beta": 1.5,
   "ndcg_val": 0.08185289919422238,
   "ndcg_kiem_tra": 0.05761384423011946,
   "coverage_kiem_tra": 0.05970054662124693,
   "so_item_kiem_tra": 7536
  },
  {
   "beta": 2.0,
   "ndcg_val": 0.06298247737593304,
   "ndcg_kiem_tra": 0.043894003335355106,
   "coverage_kiem_tra": 0.05811613720985503,
   "so_item_kiem_tra": 7336
  }
 ],
 "luoi": [
  {
   "rank": 50,
   "reg_param": 0.01,
   "alpha": 40.0,
   "ndcg_val": 0.07708678217716598,
   "ndcg_kiem_tra": 0.058121390307330184,
   "ndcg_ca_thang": 0.06684048079506107,
   "recall_kiem_tra": 0.08070917962975956,
   "so_giay": 524.6
  },
  {
   "rank": 50,
   "reg_param": 0.1,
   "alpha": 40.0,
   "ndcg_val": 0.07630667662798375,
   "ndcg_kiem_tra": 0.057405377777850534,
   "ndcg_ca_thang": 0.06619584203586693,
   "recall_kiem_tra": 0.08011832152067155,
   "so_giay": 503.5
  },
  {
   "rank": 50,
   "reg_param": 1.0,
   "alpha": 40.0,
   "ndcg_val": 0.0788621142300091,
   "ndcg_kiem_tra": 0.057901535215392604,
   "ndcg_ca_thang": 0.06801500098427876,
   "recall_kiem_tra": 0.08065770309863081,
   "so_giay": 452.8
  }
 ],
 "so_phut": 31.0
}
```
