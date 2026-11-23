## Kiểm định A/B theo user — gold.nhat_ky_hien_thi phiên bản 51 (chỉ traffic mô phỏng)

| Nhóm | User | Impression | Click | Mua | Impression/user |
|---|---|---|---|---|---|
| A | 4,310 | 175,960 | 3,685 | 412 | 40.8 |
| B | 4,282 | 174,200 | 3,446 | 417 | 40.7 |

| Chỉ số | Tỷ lệ A | Tỷ lệ B | Hiệu tương đối | p theo impression | p theo cụm user | p bootstrap user | KTC 95 % hiệu tương đối (bootstrap) | Design effect |
|---|---|---|---|---|---|---|---|---|
| ctr | 2.0942% | 1.9782% | -5.54% | 0.0151 | 0.0372 | 0.0290 | [-10.46% ; -0.74%] | 1.36 |
| chuyen_doi | 0.2341% | 0.2394% | +2.24% | 0.7499 | 0.7560 | 0.7510 | [-10.62% ; +17.36%] | 1.05 |

```json
{
 "ctr": {
  "theo_impression": {
   "z": -2.4306995456424842,
   "p_value": 0.015069705320609965
  },
  "theo_cum_user": {
   "ty_le_a": 0.02094225960445556,
   "ty_le_b": 0.01978185993111366,
   "hieu": -0.001160399673341899,
   "sai_so_chuan": 0.0005568945329042054,
   "z": -2.08369736957268,
   "p_value": 0.037187703652821306
  },
  "bootstrap_user": {
   "ty_le_a": 0.02094225960445556,
   "ty_le_b": 0.01978185993111366,
   "hieu": -0.001160399673341899,
   "ci_hieu": [
    -0.0022326239402716327,
    -0.00015478816577178215
   ],
   "hieu_tuong_doi": -0.055409478024759984,
   "ci_hieu_tuong_doi": [
    -0.10462757253493975,
    -0.0074042157204316774
   ],
   "p_value": 0.029,
   "so_lan": 2000,
   "muc_tin_cay": 0.95
  },
  "he_so_thiet_ke": 1.3612014901548417
 },
 "chuyen_doi": {
  "theo_impression": {
   "z": 0.3187570325371229,
   "p_value": 0.7499107629472263
  },
  "theo_cum_user": {
   "ty_le_a": 0.002341441236644692,
   "ty_le_b": 0.0023938002296211252,
   "hieu": 5.235899297643346e-05,
   "sai_so_chuan": 0.0001685186053513227,
   "z": 0.3107015564677678,
   "p_value": 0.7560275138922433
  },
  "bootstrap_user": {
   "ty_le_a": 0.002341441236644692,
   "ty_le_b": 0.0023938002296211252,
   "hieu": 5.235899297643346e-05,
   "ci_hieu": [
    -0.00026769223982293493,
    0.0003767742241881011
   ],
   "hieu_tuong_doi": 0.02236186505857581,
   "ci_hieu_tuong_doi": [
    -0.10616987470797436,
    0.17362106700380678
   ],
   "p_value": 0.751,
   "so_lan": 2000,
   "muc_tin_cay": 0.95
  },
  "he_so_thiet_ke": 1.0524092569572916
 },
 "quy_mo": {
  "A": {
   "so_user": 4310,
   "so_hien_thi": 175960,
   "so_click": 3685,
   "so_mua": 412
  },
  "B": {
   "so_user": 4282,
   "so_hien_thi": 174200,
   "so_click": 3446,
   "so_mua": 417
  }
 },
 "phien_ban_delta_du_lieu": 51,
 "chi_mo_phong": true
}
```
