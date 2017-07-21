/**
 * Created by Roberto on 1/8/2017.
 */

var chart_place = 'edge_chart';
var freeze = false;
var tag_target;
var diameter = 380,
    radius = diameter / 2,
    innerRadius = radius - 80;


var cluster = d3.layout.cluster()
    .size([360, innerRadius])
    .sort(null)
    .value(function(d) { return d.size; });

var bundle = d3.layout.bundle();

var margin ={
    top: 60,
    botton: 10,
    right: 50,
    left: 30
};

var line = d3.svg.line.radial()
    .interpolate("bundle")
    .tension(.85)
    .radius(function(d) { return d.y; })
    .angle(function(d) { return d.x / 180 * Math.PI; });

var svg = d3.select("#" + chart_place ).append("svg")
    .attr("width", diameter + 110)
    .attr("height", diameter + 170)
  .append("g")
    .attr("transform", "translate(" + (radius + margin.left) + "," + (radius + margin.top) + ")");

var link = svg.append("g").selectAll(".link"),
    node = svg.append("g").selectAll(".node");

colors1 = ['#73F689', '#006400'];
var colorScale1 = d3.scale.linear()
              .domain([0.5, 1])
              .range(colors1);

colors2 = ['#C00000', '#FF9CA8'];
var colorScale2 = d3.scale.linear()
              .domain([-1,-0.5])
              .range(colors2);


var data_url = '/correlation_data';
if(start_time != undefined && end_time != undefined){
    data_url = data_url + '?start_time=' + start_time.toISOString()
        + '&end_time=' +  end_time.toISOString()
}

queue()
    .defer(d3.json, data_url)
    .await(make_edge_chart);

function make_edge_chart(error, json_values) {

    var classes = json_values["data"];
    var ini_data = json_values["start_time"];
    var end_data =json_values["end_time"];
    if (error) throw error;

    var nodes = cluster.nodes(packageHierarchy(classes)),
        links = packageImports(nodes);

    link = link
        .data(bundle(links))
        .enter().append("path")
        .each(function(d) { d.source = d[0], d.target = d[d.length - 1]; })
        .attr("class", "link")
        .attr("d", line);

    node = node
        .data(nodes.filter(function(n) { return !n.children; }))
        .enter().append("text")
        .attr("class", "node")
        .attr("dy", ".31em")
        .attr("transform", function(d) { return "rotate(" + (d.x - 90) + ")translate(" + (d.y + 8) + ",0)" + (d.x < 180 ? "" : "rotate(180)"); })
        .style("text-anchor", function(d) { return d.x < 180 ? "start" : "end"; })
        .text(function(d) { return d.key; })
        .on("mouseover", mouseovered)
        .on("mouseout", mouseouted)
        .on("click",click_event);

    draw_legend();
    update_title_chart(chart_place + '_title',"Average correlation, " +
        "from " + ini_data + " to " + end_data );
    ready_button_icon(chart_place + '_update_button');
}


function draw_legend(){

    ///////////////////////////////////////////////////////////////////////////
    //////////////// Drawing the legend ///////////////////////
    ///////////////////////////////////////////////////////////////////////////

    var size_leg_x = 45;
    var size_leg_y = 10;
    // # LEGEND 1
    legend_data = [];
    var ix = 0;
    for(i=0.4; i <= 1; i +=  0.1){
        legend_data[ix]={
            index:i,
            label: (i>=0.5)? i.toFixed(1): "< 0.5"
        };
        ix += 1;
    }
    //Color Legend container
    var legendsvg = svg.append("g")
        .attr("class", "legendWrapper")
        .attr("transform", "translate(" + (-diameter/2 + 5) + "," + (diameter/2 + 30) + ")");

    //Append title
    legendsvg.append("text")
        .attr("class", "legendTitle")
        .attr("x", 0)
        .attr("y", 5)
        .style("text-anchor", "left")
        .text("Legend:");

    var legend_Map1 = legendsvg.selectAll(".legend_1")
        .data(legend_data)
        .enter().append("rect")
        .attr("x", function (d,i) {
            return (i ) * size_leg_x ;
        })
        .attr("y", 10)
        .attr("class", "hour bordered")
        .attr("width", size_leg_x)
        .attr("height", size_leg_y)
        .style("stroke", "#386da0")
        .style("fill", function (d) {
            return colorScale1(d.index);
        });

      var Legend_Labels_1 = legendsvg.selectAll(".Legend_Label_1")
        .data(legend_data)
        .enter().append("text")
        .text(function (d) {
            return d.label ;
        })
        .attr("x", function (d,i) {
            return i * size_leg_x ;
        })
        .attr("y", size_leg_y + 28)
        .attr("class", "legendLabel")
        .style("text-anchor", "middle")
        .attr("transform", "translate(" + size_leg_x  / 2 + ", -6)");

    // # LEGEND 2
    var legend_data = [];
    var index = 0;
    for(var i=-0.4; i >= -1; i -=  0.1){
        legend_data[index]={
            index:i,
            label: (i<=-0.5)? i.toFixed(1): "> -0.5"
        };
        index += 1;
    }

    var legend_Map2 = legendsvg.selectAll(".legend_2")
        .data(legend_data)
        .enter().append("rect")
        .attr("x", function (d,i) {
            return (i ) * size_leg_x ;
        })
        .attr("y", 42)
        .attr("class", "hour bordered")
        .attr("width", size_leg_x)
        .attr("height", size_leg_y)
        .style("stroke", "#386da0")
        .style("fill", function (d) {
            return colorScale2(d.index);
        });

    var Legend_Labels_2 = legendsvg.selectAll(".Legend_Label_2")
        .data(legend_data)
        .enter().append("text")
        .text(function (d) {
            return d.label ;
        })
        .attr("x", function (d,i) {
            return i * size_leg_x ;
        })
        .attr("y", size_leg_y + 60)
        .attr("class", "legendLabel")
        .style("text-anchor", "middle")
        .attr("transform", "translate(" + size_leg_x  / 2 + ", -6)");
}

function mouseovered(d) {
    if(freeze == false) {
        // Init all the nodes source and target equal to false
        var d_key = d.name;
        node
            .each(function (n) {
                n.target = n.source = false;
            });

        link
            .classed("link--target", function (l) {
                    if (l.target === d)
                        return l.source.source = true;
                }
            )
            .style("stroke", function (l) {
                var f = l.source[d_key];
                if (f > 0) {
                    return colorScale1(l.source[d_key])
                } else {
                    return colorScale2(l.source[d_key])
                }
            })
            .style("stroke-opacity", function (l) {
                if (l.target === d) {
                    //return Math.abs(l.source[d_key]);
                    return 1;
                } else {
                    return 0
                }
            })
            .filter(function (l) {
                return l.target === d;
            })
            .each(function () {
                this.parentNode.appendChild(this);
            });

        node
            .classed("node--target", function (n) {
                return n.target;
            })
            .classed("node--source", function (n) {
                return n.source;
            });
    }else{
        // Filter values in the heat chart
        var aux = (d.name).split('#');
        var filter_category = aux[0];
        if(d.source) {
            data_url = '/correlation_table_data' + "?list_projection=" + tag_target +
            '&filter_category=' + filter_category;
        }else{
            data_url = '/correlation_table_data' + "?list_projection=" + tag_target;
        }
        if (start_time != undefined && end_time != undefined) {
            data_url = data_url + "&start_time=" + start_time.toISOString()
                    + "&end_time=" + end_time.toISOString();

            }
        queue()
            .defer(d3.json, data_url)
            .await(make_heat_map_2);
    }
}

function mouseouted(d) {
    if(freeze == false){
        link
            .classed("link--target", false)
            .classed("link--source", false);

        node
            .classed("node--target", false)
            .classed("node--source", false);
    }
}

function click_event(d) {
    if(freeze == false){
        tag_target = d['tagname'];
        data_url = '/correlation_table_data' + "?list_projection=" + tag_target;
        if (start_time != undefined && end_time != undefined ){
            data_url = data_url + "&start_time=" + start_time.toISOString()
            + "&end_time=" + end_time.toISOString() ;

        }
        queue()
        .defer(d3.json, data_url)
        .await(make_heat_map_2);
        freeze = !freeze;
        d.target = true;
    }else{
        freeze = !freeze;
        mouseovered(d);
    }

}


d3.select(self.frameElement).style("height", diameter + "px");

// Construction of classes according to the children
function packageHierarchy(classes) {
  var map = {};

  // This function allows to find a node based on the name of the children
  // It retuns a node if was found
  function find(name, data) {
    var node = map[name], i;
    if (!node) {
      node = map[name] = data || {name: name, children: []};
      if (name.length) {
        node.parent = find(name.substring(0, i = name.lastIndexOf("#")));
        node.parent.children.push(node);
        node.key = name.substring(i + 1);
      }
    }
    return node;
  }

  classes.forEach(function(d) {
    find(d.name, d);
  });

  return map[""];
}

// Return a list of imports for the given array of nodes.
function packageImports(nodes) {
  var map = {},
      imports = [];

  // Compute a map from name to node.
  nodes.forEach(function(d) {
    map[d.name] = d;
  });

  // For each import, construct a link from the source to target node.
  nodes.forEach(function(d) {
    if (d.imports) d.imports.forEach(function(i) {
      imports.push({source: map[d.name], target: map[i]});
    });
  });

  return imports;
}

function update_edge_chart() {
    var data_url = '/correlation_data';
    var data_url_heat = '/correlation_table_data';
    if (start_time != undefined && end_time != undefined) {
        data_url = data_url + '?start_time=' + start_time.toISOString()
            + '&end_time=' + end_time.toISOString()
        data_url_heat = data_url_heat  + '?start_time=' + start_time.toISOString()
            + '&end_time=' + end_time.toISOString()
    }
    d3.select("#" + chart_place ).select("svg").remove();
    svg = d3.select("#" + chart_place ).append("svg")
        .attr("width", diameter + 110)
        .attr("height", diameter + 170)
        .append("g")
        .attr("transform", "translate(" + (radius + margin.left) + "," + (radius + margin.top) + ")");

    link = svg.append("g").selectAll(".link"),
    node = svg.append("g").selectAll(".node");

    queue()
        .defer(d3.json, data_url)
        .await(make_edge_chart);

      queue()
            .defer(d3.json, data_url_heat)
            .await(make_heat_map_2);

}
