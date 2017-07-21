///////////////////////////////////////////////////////////////////////////
//////////////////// Set up and initiate svg containers ///////////////////
///////////////////////////////////////////////////////////////////////////	

var data_url = '/quality_data';
var tags;
var timestamp;
var states = ['normal','flat', 'outlier', 'nan'];
var id_states = [0,1,2,3];
var color_value = ["#FFFFFF", "#3788B8", "#F1C866", '#AA178E' ];

queue()
    .defer(d3.json, data_url)
    .await(make_heat_map);

function make_heat_map(error, json_value) {

    tags  = json_value['tags'];
    timestamp = json_value['timestamp'];
    data = json_value['data'];

    //var dates = d3.range(timestamp.length);
    var dates = timestamp;

    var margin = {
        top: 100,
        right: 50,
        bottom: 0,
        left: 140
    };
// determine the size of the chart
// width = Math.max(Math.min(window.innerWidth, 1200), 500) - margin.left - margin.right
    var gridSizeX = Math.max(2,Math.floor(window.innerWidth - margin.right - margin.left)/timestamp.length);
    var gridSizeY =
        Math.min(Math.max(15, Math.floor((window.innerHeight - margin.top -margin.bottom - 100) / tags.length)),30);
    var width = gridSizeX * timestamp.length ;
    var height = gridSizeY * tags.length + margin.bottom + margin.top;
    var legendElementWidth = gridSizeY*2;
    var size_x = gridSizeX * timestamp.length;
    var size_y = gridSizeY * tags.length;
    //height = gridSizeX * (tags.length+2);

//SVG container
    var svg = d3.select('#heatmap')
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

   /* var colorScale = d3.scale.linear()
        .domain([0, d3.max(data, function (d) {
            return d.value;
        }) , d3.max(data, function (d) {
            return d.value;
        })])
        .range(["#FFFFFF", "#07FF38", "#F1C866"])*/

    var colorScale = d3.scale.linear()
        .domain(id_states)
        .range(color_value);

    //.interpolate(d3.interpolateHcl);

    var tagLabels = svg.selectAll(".tagLabel")
        .data(tags)
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

    var ticks_on = Math.max( Math.floor((timestamp.length-3)/10),1);
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
            return colorScale(d.value);
        });

    var lines_h = svg.selectAll(".line_h")
        .data(tags)
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

//Append title to the top
    svg.append("text")
        .attr("class", "title")
        .attr("x", width / 2)
        .attr("y", -50)
        .style("text-anchor", "middle")
        .text("Data Quality Screening");


///////////////////////////////////////////////////////////////////////////
//////////////// Drawing the legend ///////////////////////
///////////////////////////////////////////////////////////////////////////

    var size_leg_x = 50;
    var size_leg_y = 5;
    var legendWidth = Math.min(id_states.length*size_leg_x, 300);
    //Color Legend container
    var legendsvg = svg.append("g")
        .attr("class", "legendWrapper")
        .attr("transform", "translate(" + (0) + "," + (gridSizeY * tags.length + 20) + ")");
    var legend_data = [];
    id_states.forEach(function (item,index) {
        legend_data[index]={
            index:index,
            label:states[index]
        }
    });
    //Append title
    legendsvg.append("text")
        .attr("class", "legendTitle")
        .attr("x", 0)
        .attr("y", 5)
        .style("text-anchor", "left")
        .text("Legend:");


    var legend_Map = legendsvg.selectAll(".legend")
        .data(id_states)
        .enter().append("rect")
        .attr("x", function (d) {
            return (d ) * 50;
        })
        .attr("y", 10)
        .attr("class", "hour bordered")
        .attr("width", size_leg_x)
        .attr("height", size_leg_y)
        .style("stroke", "#386da0")
        .style("fill", function (d) {
            return colorScale(d);
        });

      var Legend_Labels = legendsvg.selectAll(".Legend_Label")
        .data(legend_data)
        .enter().append("text")
        .text(function (d) {
            return d.label ;
        })
        .attr("x", function (d) {
            return d.index * size_leg_x ;
        })
        .attr("y", size_leg_y +30)
        .style("text-anchor", "middle")
        .attr("transform", "translate(" + size_leg_x  / 2 + ", -6)")
        .attr("class", function (d, i) {
            //return ((i % 1000 == 0) ? "timeLabel mono axis axis-worktime" : "text no-text");
            return "timeLabel mono axis axis-worktime";
        });
    /*var countScale = d3.scale.linear()
        .domain([0, d3.max(data, function (d) {
            return d.value;
        })])
        .range([0, width])*/
 /*   var countScale = d3.scale.linear()
        .domain(id_states)
        .range([0, width/2])
*/

//Calculate the variables for the temp gradient
/*    var numStops = 3;
    countRange = countScale.domain();
    countRange[2] = countRange[1] - countRange[0];
    countPoint = [];
    for (var i = 0; i < numStops; i++) {
        countPoint.push(i * countRange[2] / (numStops - 1) + countRange[0]);
    }//for i

//Create the gradient
    svg.append("defs")
        .append("linearGradient")
        .attr("id", "legend-traffic")
        .attr("x1", "0%").attr("y1", "0%")
        .attr("x2", "100%").attr("y2", "0%")
        .selectAll("stop")
        .data(d3.range(numStops))
        .enter().append("stop")
        .attr("offset", function (d, i) {
            return countScale(countPoint[i]) / width;
        })
        .attr("stop-color", function (d, i) {
            return colorScale(countPoint[i]);
        });

///////////////////////////////////////////////////////////////////////////
////////////////////////// Draw the legend ////////////////////////////////
///////////////////////////////////////////////////////////////////////////

    var legendWidth = Math.min(width * 0.8, 400);
//Color Legend container
    var legendsvg = svg.append("g")
        .attr("class", "legendWrapper")
        .attr("transform", "translate(" + (margin.left) + "," + (gridSizeY * tags.length + 40) + ")");

//Draw the Rectangle
    legendsvg.append("rect")
        .attr("class", "legendRect")
        .attr("x", -legendWidth / 2)
        .attr("y", 0)
        //.attr("rx", hexRadius*1.25/2)
        .attr("width", legendWidth)
        .attr("height", 10)
        .style("fill", "url(#legend-traffic)");

//Append title
    legendsvg.append("text")
        .attr("class", "legendTitle")
        .attr("x", 0)
        .attr("y", -10)
        .style("text-anchor", "middle")
        .text("Legend");

//Set scale for x-axis
    /*var xScale = d3.scale.linear()
        .range([-legendWidth / 2, legendWidth / 2])
        .domain([0, d3.max(data, function (d) {
            return d.value;
        })]);*/
 /*   var xScale = d3.scale.linear()
        .range([- legendWidth / 2, -legendWidth / 6, legendWidth / 6, legendWidth / 2])
        .domain(id_states);

//Define x-axis
    var xAxis = d3.svg.axis()
        .orient("bottom")
        .ticks(4)
        //.tickFormat(formatPercent)
        .scale(xScale);

//Set up X axis
    legendsvg.append("g")
        .attr("class", "axis")
        .attr("transform", "translate(0," + (10) + ")")
        .call(xAxis);
*/
}