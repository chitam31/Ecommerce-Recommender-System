"""Hình: DAG 1 kn_pipeline_hang_ngay (phụ thuộc lấy đúng từ airflow/dags/kn_pipeline_hang_ngay.py).
Hai bản: dọc, đầy đủ chi tiết cho báo cáo (f05_dag1) và ngang, gọn cho slide (f05b_dag1_ngang)."""
from gv import kieu, ve_dot

SPARK, PY, NHANH, XAM = kieu("lo"), kieu("trang"), kieu("af", "diamond", 'style="filled"'), kieu("xam")
CHU_GIAI = """
    subgraph cluster_cg { label="Legend"; fontsize=9; color="#666666"; penwidth=0.8; style="rounded"; margin=8;
      cg1 [%s, label="Spark task [profile]", fontsize=8.5];
      cg2 [%s, label="Python task / sensor", fontsize=8.5];
      cg3 [%s, label="branch", fontsize=8.5, margin="0.02,0.02"];
      cg1 -> cg2 -> cg3 [style=invis];
    }""" % (SPARK, XAM, NHANH)

day_du = """
    sen [%(x)s, label=<<B>cho_du_lieu_ngay_moi</B><BR/>PythonSensor, mode = reschedule<BR/>poke 5 min, timeout 6 h, soft_fail<BR/>bronze _spark_metadata newer than silver _delta_log?<BR/><I>manual run with bo_qua_cho_du_lieu passes at once</I>>];
    mc  [%(p)s, label=<<B>tao_ma_chay</B><BR/>run id rYYYYMMDDTHHMMSS → XCom>];
    bs  [%(s)s, label=<<B>bronze_to_silver</B>  [lo]<BR/>incremental by day, replaceWhere>];
    tk  [%(s)s, label=<<B>thong_ke_san_pham</B>  [lo]>];
    lq  [%(s)s, label=<<B>san_pham_lien_quan</B>  [lo]>];
    cb  [%(s)s, label=<<B>chuan_bi_ma_tran</B>  [lo]<BR/>features version v>];
    hl  [%(s)s, label=<<B>huan_luyen_als</B>  [als]<BR/>grid toi_thieu (9) or day_du (27)<BR/>+ served configuration if missing<BR/>timeout 4 h>];
    dg  [%(s)s, label=<<B>danh_gia</B>  [als]<BR/>metrics, baselines, v1/v2, decision<BR/>prints JSON → XCom>];
    br  [%(b)s, fontsize=9, margin="0.02,0.02", label=<<B>ndcg_co_cai_thien</B><BR/>BranchPythonOperator<BR/>reads danh_gia JSON>];
    sg  [%(s)s, label=<<B>sinh_goi_y_toan_bo</B>  [als]<BR/>serving model on train + test<BR/>top-200 → novelty re-rank → top-20<BR/>(v2: top-40, filtered by price later)>];
    tr  [%(s)s, label=<<B>tron_goi_y</B>  [lo]<BR/>A/B templates, cold start,<BR/>new items, v2 price filter>];
    gg  [%(s)s, label=<<B>ghi_gold</B>  [lo]<BR/>MERGE gold.goi_y_nguoi_dung>];
    gc  [%(x)s, label=<<B>giu_mo_hinh_cu</B><BR/>keep the serving lists>];
    cw  [%(x)s, label=<<B>canh_bao</B><BR/>warning in the task log>];
    sen -> mc -> bs;
    bs -> tk -> lq; bs -> cb -> hl -> dg -> br;
    br -> sg [label=" cai_thien = true: no model yet,\\n or NDCG ≥ 0.99 × serving"]; sg -> tr -> gg;
    tk -> tr;
    br -> gc [label=" otherwise"]; gc -> cw;
    {rank=same; tk; cb;}
    {rank=same; sg; gc;}
""" % dict(s=SPARK, p=PY, b=NHANH, x=XAM)
ve_dot(day_du + CHU_GIAI, "f05_dag1", fontsize=10, edgesize=9)

