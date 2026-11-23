// Biểu đồ đường và cột của web, vẽ bằng Chart.js 4 (static/vendor/chart.umd.min.js, đóng gói sẵn
// trong repo để phòng lab không cần Internet). Giữ nguyên giao diện hàm cũ:
//   veDuong(idPhanTu, nhan, chuoi, tuyChon)   veCot(idPhanTu, nhan, chuoi, tuyChon)
// chuoi = [{ten: "Nhóm A", gia_tri: [..]}, ...]; nhan = nhãn trục ngang;
// tuyChon.dinhDang = hàm định dạng số trên trục tung và chú thích (mặc định 2 chữ số thập phân).
const MAU = ["#2563eb", "#d97706", "#15803d", "#b91c1c", "#7c3aed", "#0891b2"];
const _bieuDoDaVe = {};

function _veChartJs(loai, idPhanTu, nhan, chuoi, tuyChon) {
  const el = document.getElementById(idPhanTu);
  if (!el) return null;
  if (!nhan.length || !chuoi.length) { el.innerHTML = "<p class='mo'>Chưa có dữ liệu.</p>"; return null; }
  if (typeof Chart === "undefined") { el.innerHTML = "<p class='mo'>Không tải được Chart.js.</p>"; return null; }
  const dinhDang = tuyChon.dinhDang || (v => Number(v).toFixed(2));
  if (_bieuDoDaVe[idPhanTu]) _bieuDoDaVe[idPhanTu].destroy();
  el.innerHTML = "<div class='khung-bieu-do'><canvas aria-label='Biểu đồ' role='img'></canvas></div>";
  const bieuDo = new Chart(el.querySelector("canvas"), {
    type: loai,
    data: {
      labels: nhan.map(n => String(n)),
      datasets: chuoi.map((c, i) => ({
        label: c.ten,
        data: c.gia_tri.map(v => (v === undefined ? null : v)),
        borderColor: MAU[i % MAU.length],
        backgroundColor: MAU[i % MAU.length] + (loai === "bar" ? "cc" : "33"),
        borderWidth: 2,
        pointRadius: 3,
        spanGaps: true,
        tension: 0.15,
      })),
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: {mode: "index", intersect: false},
      scales: {
        x: {ticks: {autoSkip: true, maxTicksLimit: 12, maxRotation: 0}},
        y: {beginAtZero: true, ticks: {callback: v => dinhDang(v)}},
      },
      plugins: {
        legend: {position: "bottom"},
        tooltip: {callbacks: {label: ctx => ctx.dataset.label + ": " + (ctx.parsed.y === null ? "-" : dinhDang(ctx.parsed.y))}},
      },
    },
  });
  _bieuDoDaVe[idPhanTu] = bieuDo;
  return bieuDo;
}

function veDuong(idPhanTu, nhan, chuoi, tuyChon = {}) {
  return _veChartJs("line", idPhanTu, nhan, chuoi, tuyChon);
}

function veCot(idPhanTu, nhan, chuoi, tuyChon = {}) {
  return _veChartJs("bar", idPhanTu, nhan, chuoi, tuyChon);
}
