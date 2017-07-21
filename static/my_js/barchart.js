/**
 * Created by Roberto on 10/29/2016.
 */
// Global variables

var dateDim = ndx.dimension(function(d) { return d["date_posted"]; });
var numProjectsByDate = dateDim.group();
var minDate = dateDim.bottom(1)[0]["timestamp"];
var maxDate = dateDim.top(1)[0]["timestamp"];
var timeChart = dc.barChart("#time-table_chart");
timeChart
		.width(600)
		.height(160)
		.margins({top: 10, right: 50, bottom: 30, left: 50})
		.dimension(dateDim)
		.group(numProjectsByDate)
		.transitionDuration(500)
		.x(d3.time.scale().domain([minDate, maxDate]))
		.elasticY(true)
		.xAxisLabel("Year")
		.yAxis().ticks(4);