import { panelService } from "../api/panel.service.js";

function imprimirGrafica(){
    var options = {
            series: [100, 70, 80, 300, 28],
            chart: {
            width: 380,
            type: 'pie',
            },
            labels: ['ADSO', 'ALIMENTOS', 'COCINA', 'DEPORTIVO', 'MESA Y BAR'],
            responsive: [{
            breakpoint: 480,
            options: {
                chart: {
                width: 200
                },
                legend: {
                position: 'bottom'
                }
            }
            }]
        };

        var chart = new ApexCharts(document.querySelector("#aprendicesPrograma"), options);
        chart.render();
}

async function Init(){
    
    console.log("cargando panel.js");
    const historico = await panelService.getHistorico;
    console.log(historico); 
    imprimirGrafica();
}

export { Init }