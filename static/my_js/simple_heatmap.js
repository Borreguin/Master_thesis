//Based on the heatmap example of: http://blockbuilder.org/milroc/7014412

///////////////////////////////////////////////////////////////////////////
//////////////////// Set up and initiate svg containers ///////////////////
///////////////////////////////////////////////////////////////////////////	

function draw_heatmap(error, id_svg, json_value){

	//d3.select("#CO2_bbc").style("fill", "white");
	//var days = ["1a", "2a", "3a", "4a", "5a", "6a", "7a"];
	var days = json_value['days'],
		heatmap_data = json_value['data'],
		max_value = json_value['max'],
		min_value = json_value['min'],
		times = d3.range(24),
		units_value = '';

	if(json_value['units'] != null){
		units_value = json_value['units'];
	}

	var margin = {
		top: 0,
		right: 0,
		bottom: 0,
		left: 0
	};

	//var width = Math.max(Math.min(window.innerWidth, 1000), 500) - margin.left - margin.right - 20,
	//var width = 400,
	var width = d3.select(id_svg).attr("width"),
		height = d3.select(id_svg).attr("height");
		
	//var	gridSize = Math.floor(width / times.length);
	var	gridSizeX = (width - 1)/ times.length,
		gridSizeY = (height)/ days.length;
	
	//SVG container
	var svg = d3.select(id_svg)
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

	var colorScale = create_scale(max_value, min_value);

	var heatMap = svg.selectAll(".hour")
		.data(heatmap_data)
		.enter().append("rect")
		.attr("x", function(d) { return (d.hour) * gridSizeX; })
		.attr("y", function(d) { return (d.day) * gridSizeY; })
		.attr("class", "hour bordered")
		.attr("width", gridSizeX)
		.attr("height", gridSizeY)
		.style("stroke", "black")
		//.style("stroke-opacity", 0.6)
		.style("fill", function(d) { return colorScale(d.value); })
		.append("title").text(function(d) {
		    return "Hour: " + d.hour + "\n" + d.value + " " + units_value +
		            "\nDays: " + days[d.day]});

}

function create_scale(max_value, min_value){

    var delta = max_value - min_value;
	var colorScale = d3.scale.linear()
		.domain([min_value, min_value + delta/5, min_value + 2*delta/5, min_value + 3*delta/5, min_value + 4*delta/5,  max_value])
		.range(["#251EFC",    "#80B2FC",    "#3CD650" ,     "#EBE85B",      "#EB7632",     "#AC0230"  ])
		//window.alert([min_value, min_value + delta/5, min_value + 2*delta/5, min_value + 3*delta/5, min_value + 4*delta/5,  max_value])
		.interpolate(d3.interpolateHcl);

    return colorScale;

}

/*var json_value = {
	data: 
	[{day:2,hour:12,value:700},{day:0,hour:1,value:141},{day:0,hour:1,value:134},{day:5,hour:1,value:174},{day:3,hour:1,value:131},{day:6,hour:1,value:333},{day:7,hour:1,value:311},{day:2,hour:2,value:79},{day:4,hour:2,value:99},{day:1,hour:2,value:117},{day:5,hour:2,value:123},{day:3,hour:2,value:92},{day:6,hour:2,value:257},{day:7,hour:2,value:293},{day:2,hour:3,value:55},{day:4,hour:3,value:73}],
	max: 700,
	min: 10,
	days: ["1 Sep", "2 Sep", "3 Sep", "4 Sep", "5a", "6a", "7a"]
	
}
	
draw_heatmap(400, '#trafficAccidents', json_value)*/
