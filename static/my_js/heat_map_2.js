///////////////////////////////////////////////////////////////////////////
//////////////////// Set up and initiate svg containers ///////////////////
///////////////////////////////////////////////////////////////////////////

var data_url = '/correlation_table_data';
var tag_alias_list;
var tag_title;
var tagname_list;
var timestamp;
var tagname;
var heat_chart_place = 'heatmap_chart';

// expected values from -1 to 1
var colors1 = ['#73F689', '#006400'];
var colors2 = ['#C00000', '#FF9CA8'];
var domain1 = [0.5,1];
var domain2 = [-1,-0.5];


queue()
    .defer(d3.json, data_url)
    .await(make_heat_map_2);

function make_heat_map_2(error, json_value) {

    tag_alias_list  = json_value['tag_alias_list'];
    tagname_list = json_value['tagname_list'];
    timestamp = json_value['timestamp'];
    data = json_value['data'];
    tag_title= json_value["title"];
    tagname = json_value["tagname"]

    //var dates = d3.range(timestamp.length);
    var dates = timestamp;

    var margin = {
        top: 30,
        right: 10,
        bottom: 0,
        left: 140
    };
// determine the size of the chart
// width = Math.max(Math.min(window.innerWidth, 1200), 500) - margin.left - margin.right
    var gridSizeX = Math.max(2,Math.floor(window.innerWidth*0.6 - margin.right - margin.left)/timestamp.length);
    var gridSizeY =
        Math.min(Math.max(15, Math.floor((window.innerHeight - margin.top -margin.bottom - 100) / tag_alias_list.length)),30);
    var width = gridSizeX * timestamp.length ;
    var height = gridSizeY * tag_alias_list.length + margin.bottom + margin.top;
    var legendElementWidth = gridSizeY*2;
    var size_x = gridSizeX * timestamp.length;
    var size_y = gridSizeY * tag_alias_list.length;
    //height = gridSizeX * (tags.length+2);

    d3.select('#'+ heat_chart_place).select("svg").remove();

//SVG container
    var svg = d3.select('#'+ heat_chart_place)
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");



//Reset the overall font size
    var newFontSize = width * 62.5 / 900;
    d3.select("html").style("font-size", newFontSize + "%");

///////////////////////////////////////////////////////////////////////////
//////////////////////////// Draw Heatmap /////////////////////////////////
///////////////////////////////////////////////////////////////////////////

//Based on the heatmap example of: http://blockbuilder.org/milroc/7014412

    var colorScale1 = d3.scale.linear()
              .domain(domain1)
              .range(colors1);

    var colorScale2 = d3.scale.linear()
              .domain(domain2)
              .range(colors2);

    var tagLabels = svg.selectAll(".tagLabel")
        .data(tag_alias_list)
        .enter().append("text")
        .text(function (d) {
            return d;
        })
        .attr("x", 0)
        .attr("y", function (d, i) {
            return i * gridSizeY;
        })
        .style("text-anchor", "end")
        .attr("transform", "translate(-6," + gridSizeY / 1.5 + ")")
        .attr("class", function (d, i) {
            return ((i % 4 == 0) ? "dayLabel mono axis axis-workweek" : "dayLabel mono axis");
        });

    var ticks_on = Math.max( Math.floor((timestamp.length-3)/6),1);
    var dateLabels = svg.selectAll(".timeLabel")
        .data(dates)
        .enter().append("text")
        .text(function (d, i) {
            return ((i % ticks_on) ==0)? d : '' ;
        })
        .attr("x", function (d, i) {
            return i * gridSizeX;
        })
        .attr("y", 0)
        .style("text-anchor", "middle")
        .attr("transform", "translate(" + gridSizeX / 2 + ", -6)")
        .attr("class", function (d, i) {
            return "timeLabel mono axis axis-worktime";
        });

    svg.append("rect")
        .attr("x", -1)
        .attr("y", -1)
        .attr("class", "hour bordered")
        .attr("width", size_x + 1.5)
        .attr("height", size_y + 2)
        .style("stroke", "black")
        .style("fill", "white");

     var tip = d3.tip()
      .attr("class", "d3-tip")
      .offset([-10, 0])
      .html(function(d) {
        return  timestamp[d.date] +
            "<br>" + tag_alias_list[d.tag] + ":</br> " + d.value ;
      });
    svg.call(tip);

   var heatMap = svg.selectAll(".date")
        .data(data)
        .enter().append("rect")
        .attr("x", function (d) {
            return (d.date ) * gridSizeX;
        })
        .attr("y", function (d) {
            return (d.tag ) * gridSizeY;
        })
        .attr("class", "hour bordered")
        .attr("width", gridSizeX)
        .attr("height", gridSizeY)
        .style("stroke-opacity", 0.1)
        .style("fill", function (d) {
            if(d.value > 0) {
                return colorScale1(d.value);
            }else{
                return colorScale2(d.value);
            }
        })
       .attr("id",function (d) {
           return d.tag + "-" + d.date;
       })
       .on("click", observe_correlation)
       .on("mouseover", tip.show)
       .on("mouseout", tip.hide);

    var lines_h = svg.selectAll(".line_h")
        .data(tag_alias_list)
        .enter().append("line")
        .attr("x1", 0)
        .attr("y1", function (d,i) {
            return (i) * gridSizeY;
        })
        .attr("x2", size_x )
        .attr("y2", function (d,i) {
            return (i) * gridSizeY;
        })
        .attr("stroke-width",1)
        .attr("stroke","#AAAAAA")
        /*.attr("stroke",function (d,i) {
            return ((i % ticks_on) ==0)? "blue" : "#AAAAAA" ;})*/
        .style("stroke-opacity", 1);

     var lines_v = svg.selectAll(".line_v")
        .data(dates)
        .enter().append("line")
        .attr("y1", 0)
        .attr("x1", function (d,i) {
            return (i) * gridSizeX;
        })
        .attr("y2", size_y )
        .attr("x2", function (d,i) {
            return (i) * gridSizeX;
        })
        .attr("stroke-width",0.7)
        .attr("stroke", function (d,i) {
            return ((i % ticks_on) ==0)? "black" : "#AAAAAA" ;})
        .style("stroke-opacity", 0.6);

    update_title_chart(heat_chart_place + '_title', tag_title +
        " vs. Temperature - Correlation table by day");
    ready_button_icon(heat_chart_place + '_update_button');
}

function observe_correlation(d){

    var window_name = tag_alias_list[d.tag];
    var projection_list = [];
    projection_list[0] = tagname;
    projection_list[1] = tagname_list[d.tag];

    var x = this;
    //var m = d3.select('#'+ d.tag + "-" + d.date)
    d3.select(this)
        .style("stroke-opacity", 1)
        .style("stroke", "yellow")
        .style("stroke-width", 3);

    var url = '/scatter';

    if(timestamp[d.date+1]!=undefined){
        url = url + "?list_projection=" + projection_list +
            "&start_time=" + timestamp[d.date] +
            "&end_time=" + timestamp[d.date+1];
    }else{
        url = url + "?list_projection=" + projection_list +
            "&start_time=" + timestamp[d.date] +
            "&end_time=" + end_time.toISOString();
    }
    url = url + "&c_value="  + d.value;
     var new_window = window.open(url, window_name + timestamp[d.date], "height=400,width=400,status=yes,toolbar=no,menubar=no,location=no,scrollbars=no");

}
