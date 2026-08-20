// Draws the hourly temperature chart on the daily report page. All data is
// embedded server-side in #chart-data; the only external resource is chart.js
// from its CDN. If that script did not load (offline), show the fallback note;
// the hourly table below the chart carries the same numbers.
(function () {
  "use strict";

  var payloadEl = document.getElementById("chart-data");
  if (!payloadEl) return;

  var canvas = document.getElementById("hourly-chart");
  var fallback = document.getElementById("chart-fallback");
  if (typeof Chart === "undefined") {
    if (fallback) fallback.hidden = false;
    if (canvas) canvas.hidden = true;
    return;
  }

  var payload = JSON.parse(payloadEl.textContent);
  var root = getComputedStyle(document.documentElement);
  var ink = root.getPropertyValue("--ink").trim() || "#e9e4d8";
  var dim = root.getPropertyValue("--dim").trim() || "#9aa8a4";
  var faint = root.getPropertyValue("--faint").trim() || "#67756f";
  var signal = root.getPropertyValue("--signal").trim() || "#f2a54a";
  var teal = root.getPropertyValue("--teal").trim() || "#63b3a5";
  var grid = root.getPropertyValue("--hairline-soft").trim() || "#1a282e";

  Chart.defaults.font.family = "'IBM Plex Mono', ui-monospace, monospace";
  Chart.defaults.font.size = 11;
  Chart.defaults.color = dim;

  var datasets = [
    {
      label: "temperature c",
      data: payload.temperature,
      borderColor: signal,
      backgroundColor: "rgba(242, 165, 74, 0.10)",
      borderWidth: 2,
      pointRadius: 0,
      tension: 0.35,
      fill: true,
    },
    {
      label: "apparent c",
      data: payload.apparent,
      borderColor: teal,
      borderWidth: 1.5,
      borderDash: [5, 4],
      pointRadius: 0,
      tension: 0.35,
    },
  ];

  if (payload.flagHours.length > 0) {
    datasets.push({
      label: "flagged hours",
      data: payload.temperature.map(function (value, index) {
        return payload.flagHours.indexOf(index) >= 0 ? value : null;
      }),
      showLine: false,
      pointStyle: "circle",
      pointRadius: 6,
      pointHoverRadius: 8,
      pointBorderWidth: 2,
      pointBackgroundColor: signal,
      pointBorderColor: "#0d1418",
    });
  }

  new Chart(canvas.getContext("2d"), {
    type: "line",
    data: { labels: payload.labels, datasets: datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      spanGaps: true,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { labels: { boxWidth: 14, boxHeight: 2 } },
        tooltip: {
          callbacks: {
            title: function (items) {
              var index = items[0].dataIndex;
              return (
                payload.labels[index] +
                " utc  (" +
                payload.localLabels[index] +
                " " +
                payload.timezone +
                ")"
              );
            },
            afterBody: function (items) {
              var notes = payload.flagNotes[items[0].dataIndex];
              return notes ? notes.slice() : [];
            },
          },
        },
      },
      scales: {
        x: {
          ticks: { color: faint, maxTicksLimit: 12, maxRotation: 0 },
          grid: { color: grid },
        },
        y: {
          ticks: {
            color: dim,
            callback: function (value) {
              return value + "\u00b0";
            },
          },
          grid: { color: grid },
        },
      },
    },
  });
})();
