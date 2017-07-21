/**
 * Created by Roberto on 1/19/2017.
 */

var tag_list = getQueryVariable('list_projection');
var start_time = getQueryVariable('start_time');
var end_time = getQueryVariable('end_time');
var c_value = getQueryVariable('c_value');
var data_scatter_url = '/data';

if(start_time != null){
    data_scatter_url = data_scatter_url + "?list_projection=" + tag_list +
            "&start_time=" + start_time +"&end_time=" + end_time;
}

var w = window.innerWidth
|| document.documentElement.clientWidth
|| document.body.clientWidth;

var h = window.innerHeight
|| document.documentElement.clientHeight
|| document.body.clientHeight;

var margin = { top: 20, right: 30, bottom: 150, left: 70 },
    outerWidth = w ,
    outerHeight = h,
    width_char = outerWidth - (margin.left + margin.right),
    height_char = outerHeight - (margin.top + margin.bottom);

var x = d3.scale.linear()
    .range([0, width_char]);

var y = d3.scale.linear()
    .range([height_char, 0]).nice();

var xTag, yTag, rCat, colorCat;
var var_tag = [], alias, unit,
    url_meta = '/metadata'+ "?list_projection=tagname,alias,units" +
            "&query=tagname:";


var data;
queue()
    .defer(d3.json, data_scatter_url)
    .await(make_scatter);

function make_scatter(error, json_values) {

    data = json_values;
    var list_keys = Object.keys(data[0]), i = 0;
    for (var idx in list_keys) {
        if (list_keys[idx] != 'timestamp') {
            var_tag[i++] = list_keys[idx];
        }
    }
    xTag = var_tag[1];
    yTag = var_tag[0];
    queue()
        .defer(d3.json,url_meta + xTag)
        .defer(d3.json,url_meta + yTag)
        .await(draw_scatter);
}

function draw_scatter(error,json_X,json_Y){
  if(alias == undefined ){
      alias = {}; unit = {};
      alias[xTag] = json_X[0]['alias'];
      alias[yTag] = json_Y[0]['alias'];
      unit[xTag] = json_X[0]['units'];
      unit[yTag] = json_Y[0]['units'];
  }

  var xMax = d3.max(data, function(d) { return +d[xTag]; }),
      xMin = d3.min(data, function(d) { return +d[xTag]; }),
      yMax = d3.max(data, function(d) { return +d[yTag]; }),
      yMin = d3.min(data, function(d) { return +d[yTag]; });

  xMax = xMax + (xMax-xMin)*0.1;
  yMax = yMax + (yMax-yMin)*0.1;

  xMin = xMin - (xMax-xMin)*0.1;
  yMin = yMin - (yMax-yMin)*0.1;

  x.domain([xMin, xMax]);
  y.domain([yMin, yMax]);

  var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom")
      .ticks(5)
      .tickSize(-height_char);

  var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left")
      .tickSize(-width_char);

  var color = d3.scale.category10();

  var tip = d3.tip()
      .attr("class", "d3-tip")
      .offset([-10, 0])
      .html(function(d) {
        return  d['timestamp'] +
            "<br>" + alias[yTag] + ":</br> " + d[yTag]+ " " + unit[yTag] +
            "<br>" + alias[xTag] + ":</br>" + d[xTag] + " " + unit[xTag]  ;
      });

  var zoomBeh = d3.behavior.zoom()
      .x(x)
      .y(y)
      .scaleExtent([0, 500])
      .on("zoom", zoom);

    d3.select("#scatter").select("svg").remove();
  var svg = d3.select("#scatter")
    .append("svg")
      .attr("width", outerWidth)
      .attr("height", outerHeight)
    .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
      .call(zoomBeh);

  svg.call(tip);

  svg.append("rect")
      .attr("width", width_char)
      .attr("height", height_char);

var title;
if(c_value != null){
    title = "R:  " + c_value  + " f:"  + start_time + " to " + end_time;
}else{
    title = "f:"  + start_time + " to " + end_time;
  }

  svg.append("text")
        .classed("axisLabel", true)
        .attr("x", width_char/2)
        .style("text-anchor", "middle")
        .text(title);


  svg.append("g")
      .classed("x axis", true)
      .attr("transform", "translate(0," + height_char + ")")
      .call(xAxis)
    .append("text")
      .classed("axisLabel", true)
      .attr("x", width_char)
      .attr("y", 40)
      .style("text-anchor", "end")
      .text(alias[xTag] + " [" +unit[xTag] + "]");

  svg.append("g")
      .classed("y axis", true)
      .call(yAxis)
    .append("text")
      .classed("axisLabel", true)
      .attr("transform", "rotate(-90)")
      .attr("y", -margin.left)
      .attr("dy", margin.left/3)
      .style("text-anchor", "end")
      .text(alias[yTag] + ' [' +unit[yTag] + ']');

  var objects = svg.append("svg")
      .classed("objects", true)
      .attr("width", width_char)
      .attr("height", height_char);

  objects.append("svg:line")
      .classed("axisLine hAxisLine", true)
      .attr("x1", 0)
      .attr("y1", 0)
      .attr("x2", width_char)
      .attr("y2", 0)
      .attr("transform", "translate(0," + height_char + ")");

  objects.append("svg:line")
      .classed("axisLine vAxisLine", true)
      .attr("x1", 0)
      .attr("y1", 0)
      .attr("x2", 0)
      .attr("y2", height_char);

  objects.selectAll(".dot")
      .data(data)
    .enter().append("circle")
      .classed("dot", true)
      //.attr("r", function (d) { return 6 * Math.sqrt(d[rCat] / Math.PI); })
      .attr("r", 2)
      .attr("transform", transform)
      .style("fill", function(d) { return 'blue'; })
      .on("mouseover", tip.show)
      .on("mouseout", tip.hide);

  var legend = svg.selectAll(".legend")
      .data(color.domain())
    .enter().append("g")
      .classed("legend", true)
      .attr("transform", function(d, i) { return "translate(0," + i * 20 + ")"; });

  legend.append("circle")
      .attr("r", 3.5)
      .attr("cx", width_char + 20)
      .attr("fill", color);

  legend.append("text")
      .attr("x", width_char + 26)
      .attr("dy", ".35em")
      .text(function(d) { return d; });

    d3.select("#Invert_Axes").on("click", change);
    d3.select("#reset").on("click", draw_scatter);

  function change() {
    if(xTag == var_tag[0]){
        xTag = var_tag[1];
        yTag = var_tag[0];
    }else{
        xTag = var_tag[0];
        yTag = var_tag[1];
    }
    draw_scatter();
  }

  function zoom() {
    svg.select(".x.axis").call(xAxis);
    svg.select(".y.axis").call(yAxis);

    svg.selectAll(".dot")
        .attr("transform", transform);
  }

  function transform(d) {
    return "translate(" + x(d[xTag]) + "," + y(d[yTag]) + ")";
  }
}

// This function gets the parameter values from the URL
function getQueryVariable(variable) {
   var query = window.location.search.substring(1);
   var vars = query.split("&");
   for (var i=0; i < vars.length; i++) {
       var pair = vars[i].split("=");
       if(pair[0] == variable) {
           return pair[1];
       }
   }
   return null;
}
