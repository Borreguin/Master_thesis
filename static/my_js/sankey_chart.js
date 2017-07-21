/**
 * Created by Roberto on 4/4/17.
 */

var units = "days";
var heat_map_width = 170;
var chart_place = "sankey_chart";
var svg;
var to_send,
    link,
    node,
    init_scale = true;

var margin = {top: 40, right: 10, bottom: 20, left: 10},
    width = 1090 - margin.left - margin.right,
    height = 700 - margin.top - margin.bottom;

var formatNumber = d3.format(",.0f"),    // zero decimal places
    format = function(d) { return formatNumber(d) + " " + units; },
    color = d3.scale.category20();


// Set the sankey diagram properties
var sankey = d3.sankey()
    .nodeWidth(20)
    .nodePadding(5)
    .size([width, height]);

var path = sankey.link();

// load the data and make the sankey diagram
function get_url(){
    build_package();
    to_send = JSON.stringify(package_to_send),
            data_url = '/sankey_data?package=' + to_send;
    return data_url;
}

queue()
    .await(update_menu_bar)
    .await(create_buttons)
    .await(load_all_variables)
    .await(get_url)
    .defer(d3.json, data_url)
    .await(make_sankey_diagram);

function make_sankey_diagram(error, json_values){

    if(error != null){console.log(error);}
    if( isEmpty(json_values)){
        update_title_chart(chart_place + '_title',"There is no data to show, "
            + "from " + start_time.toDateString() + " to " + end_time.toDateString() );
        ready_button_icon(chart_place + '_update_button');
        return false;
    }

    var nodeMap = {};
    json_values.nodes.forEach(function(x) {
		nodeMap[x.name] = x;
		});
    json_values.links = json_values.links.map(function(x) {
      return {
        source: nodeMap[x.source],
        target: nodeMap[x.target],
        value: x.value
      };
    });

    // append the svg canvas to the page
    svg = d3.select("#" + chart_place).append("svg")
        .attr("width", width + margin.left + margin.right + 300)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");



  sankey
      .nodes(json_values.nodes)
      .links(json_values.links)
      .layout(200);

// add in the links
    link = svg.append("g").selectAll(".link")
        .data(json_values.links)
        .enter().append("path")
        .attr("class", "link")
        .attr("d", path)
        .style("stroke-width", function(d) { return Math.max(1, d.dy); })
        .sort(function(a, b) { return b.dy - a.dy; });

// add the link titles
    link.append("title")
        .text(function(d) {
        return d.source.name + "->" +
                d.target.name + "\n" + format(d.value); });

// add in the nodes
    node = svg.append("g").selectAll(".node")
        .data(json_values.nodes)
        .enter().append("g")
        .attr("id", function(d) {return d.name})
        .attr("class", "node")
        .attr("transform", function(d) {
          return "translate(" + d.x + "," + d.y + ")"; })
        .attr("height", function(d) { return d.dy; })
        .attr("width", function(d)  { if(d.type == 'node'){ return sankey.nodeWidth();}
                                      else{ return heat_map_width; }})
        .call(d3.behavior.drag()
        .origin(function(d) { return d; })
        .on("dragstart", function() { this.parentNode.appendChild(this); })
        .on("drag", dragmove));

// add the rectangles for the nodes
    node.append("rect")
        .attr("height", function(d) { return d.dy; })
        .attr("width", function(d)  { if(d.type == 'node'){
                                        return sankey.nodeWidth();}
                                    else{ return heat_map_width ;}})
        .style("fill", function(d) { if(d.type == 'node') {return d.color = color(d.name.replace(/ .*/, ""));} })
        .style("stroke", function(d) { return d3.rgb(d.color).darker(2); })
        .append("title").text(function(d) { return d.name + "\n" + format(d.value); });


// add in the title for the nodes
  node.append("text")
      .attr("x", -6)
      .attr("y", function(d) { return d.dy / 2; })
      .attr("dy", ".35em")
      .attr("text-anchor", "end")
      .attr("transform", null)
      .text(function(d) {
		if(d.type == 'node'){
		return d.name; }
		})
    .filter(function(d) { return d.x < width / 2; })
      .attr("x", 6 + sankey.nodeWidth())
      .attr("text-anchor", "start");

// the function for moving the nodes
  function dragmove(d) {
    d3.select(this).attr("transform",
        "translate(" + (
        	   d.x = Math.max(0, Math.min(width - d.dx, d3.event.x))
        	) + "," + (
                   d.y = Math.max(0, Math.min(height - d.dy, d3.event.y))
            ) + ")");
    sankey.relayout();
    link.attr("d", path);
  }


    update_title_chart(chart_place + '_title',"Suffix analysis and correlation profiles, ") ;
    //+ "from " + ini_data + " to " + end_data );

    ready_button_icon(chart_place + '_update_button');

    var sax_words = json_values['sax_words'],
        to_send = JSON.stringify(package_to_send),
        data_url = '/heatmap_sax_data?package=' + to_send +
                    '&filter_sax=';

    init_scale = true;
    for(var is in sax_words){
        var new_data_url = data_url + sax_words[is];
        console.log(sax_words[is]);
        //console.log(new_data_url);
        queue()
            .defer(d3.json, new_data_url)
            .await(draw_each_heatmap);

    }

}



function draw_each_heatmap(error, map_values){

        if(error!=null){ console.log(error)}
        if(isEmpty(map_values)){return false}

        var categories = package_to_send['categories'];
        for(var ct in categories){
            error = null;
            var id_svg = '#'+ categories[ct] + '-' + map_values['sax_word'] ;
            draw_heatmap(error,id_svg, map_values[categories[ct]]);

        }

        if(init_scale){
            // add the rectangles for scale

            for(var ct in categories){
                id_svg = '#'+ categories[ct] + '-' + map_values['sax_word'] ;
                var t = d3.transform(d3.select(id_svg).attr("transform")),
                    x = t.translate[0],
                    y = t.translate[1];

              var scale = svg.append("g")
	                .attr("class", "legendWrapper")
	                .attr("transform", "translate(" + x + "," + -margin.top/3 + ")");

                scale.append("rect")
                        .attr("class", "legendRect")
                        .attr("height", 10)
                        .attr("width", heat_map_width)
                        .attr("transform", null)
                        .style("fill", "url(#legend-traffic)")
                        .style("stroke", 'black')
                        .append("title").text('Scale ' + tag_list[categories[ct]]);

                var max_value = map_values[categories[ct]]['max'],
                    min_value = map_values[categories[ct]]['min'],
                    colorScale = create_scale(max_value, min_value);



                var title = tag_alias[tag_list[categories[ct]]];
                svg.append("text")
                    .attr("x", x + heat_map_width/2)
                    .attr("y", -margin.top/1.3)
                    .attr("text-anchor", "middle")
                    .attr("transform", null)
                    .text(title)
                    .style("font-weight", "bold")
                    .style("font-size", '12px');


                var countScale = d3.scale.linear()
	                .domain([min_value, max_value ])
	                .range([0, heat_map_width])

                //Calculate the variables for the temp gradient
                var numStops = 10;
                    countRange = countScale.domain();
                    countRange[2] = countRange[1] - countRange[0];
                    countPoint = [];
                    for(var i = 0; i < numStops; i++) {
                        countPoint.push(i * countRange[2]/(numStops-1) + countRange[0]);
                    }

                //Create the gradient
                scale.append("defs")
                    .append("linearGradient")
                    .attr("id", "legend-traffic")
                    .attr("x1", "0%").attr("y1", "0%")
                    .attr("x2", "100%").attr("y2", "0%")
                    .selectAll("stop")
                    .data(d3.range(numStops))
                    .enter().append("stop")
                    .attr("offset", function(d,i) {
                        return countScale( countPoint[i] )/heat_map_width;
                    })
                    .attr("stop-color", function(d,i) {
                        return colorScale( countPoint[i] );
                    });

                //Set scale for x-axis
                var xScale = d3.scale.linear()
	                .range([10, heat_map_width-10])
	                .domain([ min_value, max_value]);

                //Define x-axis
                var xAxis = d3.svg.axis()
                      .orient("top")
                      .ticks(4)
                      .scale(xScale);
                //Set up X axis
                scale.append("g")
	                .attr("class", "axis")
	                .attr("transform", "translate(0," + 9 + ")")
	                .call(xAxis);

            }



            /*for(ct in categories){


            }*/
            init_scale = false;
        }
}

