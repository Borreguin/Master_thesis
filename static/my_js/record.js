/**
 * Created by Roberto on 10/29/2016.
 */

// Global variables:
var chart_location = 'chart1';
var json_format = 'records';
var chart = dc.dataTable("#"+ chart_location); // "#chart1" is referenced in the html file as <table id="list"></table>
var timeChart = dc.barChart("#time-chart"); //time table
var ndx; // crossfilter variable
var list_projection;
var time_query;
//---------------------------------------------------------------
//This is general information to use in the Menu ////////////////////
// if they were already used, they are saved in the local User
// so we need only to recollect this data from the local machine
var new_list_tag = loadData('_list_tag'),
    new_selected_category_button = loadData('_selected_category_button'),
    new_tag_alias = loadData('_tag_alias'),
    new_json_categories = loadData('_json_categories'),
    new_tag_category = loadData('_tag_category'),
    new_start_time = loadData('_start_time'),
    new_end_time = loadData('_end_time');

if(new_list_tag != null && new_list_tag != 'undefined'){
    if(new_list_tag!= null) list_tag =  new_list_tag;
    if(new_selected_category_button!=null) selected_category_button = new_selected_category_button;
    if(new_tag_alias!=null) tag_alias = new_tag_alias;
    if(new_json_categories!=null) json_categories = new_json_categories;
    if(new_tag_category!=null) tag_category = new_tag_category;
    if(new_start_time!=null) start_time = new Date(new_start_time);
    if(new_end_time!=null) end_time= new Date(new_end_time);
}


///////////////////////////////////////////////////////
// TODO: Put the default variables
// V022_vent02_CO2 is a default variable
// 2013-11-05 is a default time
// if is needed it to receive by URL:
//list_projection = getQueryVariable('list_projection');
// getQueryVariable('time_query');

if(list_tag == null || list_tag == 'undefined'){
    if(new_list_tag != null){
        list_projection = new_list_tag;
        list_tag = new_list_tag;
    }else{
        list_projection  = 'V022_vent02_CO2';
        list_tag = ['V022_vent02_CO2'];
    }
}else{
    list_projection =list_tag;
}
/*
if(time_query == null){
    time_query = '2013';
}*/

if(start_time == null){
    start_time = '2013-01-01';
}
if(end_time == null){
    end_time = '2013-12-20';
}


var data_url = "/data" + "?list_projection=" + list_projection + '&start_time=' + start_time.toISOString()
    + '&end_time=' + end_time.toISOString() +
    "&json_format=" + json_format ;

queue()
    .defer(d3.json, data_url)
    .await(make_list);

function make_list(error,json_list) {
    console.log(error);
    var dateFormat = d3.time.format("%Y-%m-%d %H:%M:%S");
    //Transform the timestamp to be used in my_js
    json_list.forEach(function(d) {
        d["timestamp"] = dateFormat.parse(d["timestamp"]);
        var n = d.timestamp.toTimeString();
        n = n.split(' ');
        d["hour"] = n[0];
	});
    // Get the list of attributes -> list of tag names
    //var att_list = Object.keys(json_list[0]);
    /*var tag_list = [];
    for(var idx in att_list)
    {   var attribute_value = att_list[idx];
        if( attribute_value !== "timestamp" && attribute_value !== "hour") {
           tag_list =  tag_list.concat(attribute_value);
        }}*/

    // Create the crossfilter
    ndx = crossfilter(json_list);
    //var fmt = d3.format('.2f');
    // each d represent a register(record), therefore to access to each value
    // is d['tagname'] and tagname is given by 'list_selected_buttons[x]'
    var tagDimension =
            ndx.dimension(function (d) //here d is each fact (is working as a rows)
                {
                    var values = [];
                    for(var x in list_tag){
                        values = values.concat(+d[list_tag[x]]);
                    }
                    return values;}
            ),
        grouping = function (d)
        {
            //return [d["timestamp"].toLocaleDateString()];
           // return [d["timestamp"].toISOString()];
            return [d["timestamp"].toDateString()];
        };
    // # Create dimension for time
    var dateDim = ndx.dimension(function(d) { return d["timestamp"]; }),
       // GroupByDate = dateDim.group(),
        minDate = dateDim.bottom(1)[0]["timestamp"],
        maxDate = dateDim.top(1)[0]["timestamp"],
        //valueDimension = ndx.dimension(function(d) {return +d[list_tag[0]];}),
        valueSumGroup = dateDim.group().reduceSum(function(d) {return +d[list_tag[0]];});
        /*valueSumGroup = dateDim.group().reduceSum(function(d) {
            var sum_value = 0;
            for(var x in list_tag){
                sum_value = sum_value + d[list_tag[x]];
            }
            return sum_value;}
        );*/


    timeChart
		.width(720)
		.height(80)
		.margins({top: 20, right: 10, bottom: 20, left: 40})
		.dimension(dateDim)
		.group(valueSumGroup)
		.transitionDuration(1000)
		.x(d3.time.scale().domain([minDate, maxDate]))
		.elasticY(true)
		//.xAxisLabel("Year")
        //.xAxis().ticks(5)
		.yAxis().ticks(4);
    //timeChart.render();
    var show_columns = list_tag.slice();
    show_columns.unshift('hour');
    // Creating the table chart using the 3d library
    chart
        .width(300)
        .height(200)
        .dimension(tagDimension)
        .group(grouping)
        .size(Infinity)
        .columns(show_columns)
        .sortBy(function (d) {
            return [d["timestamp"].toISOString()];
        })
        .order(d3.ascending);
    update();
   // chart.render();
    dc.renderAll();

    //-------------------------------------------//
    // TODO: This only to configure if is needed it
    //-------------------------------------------//
    ready_button_icon(chart_location + '_update_button');
    //------------------------------------------//


}
// Controls for the Table
// use odd page size to show the effect better
var ofs = 0, pag = 12;
function display() {
    d3.select('#begin')
        .text(ofs);
    d3.select('#end')
        .text(ofs+pag-1);
    d3.select('#last')
        .attr('disabled', ofs-pag<0 ? 'true' : null);
    d3.select('#next')
        .attr('disabled', ofs+pag>=ndx.size() ? 'true' : null);
    d3.select('#size').text(ndx.size());
 }
 function update() {
     chart.beginSlice(ofs);
     chart.endSlice(ofs+pag);
     display();
 }

 function next() {
    ofs += pag;
    update();
    chart.redraw();
 }
 function last() {
    ofs -= pag;
    update();
    chart.redraw();
 }

//-------------------- Used to modify the register table ------
// The following variables come from the Menu script:
function update_table() {
    if(list_tag.length > 0){
        var data_url = "/data" + "?list_projection=" + list_tag + '&start_time=' + start_time.toISOString()
            + '&end_time=' + end_time.toISOString() +
            "&json_format=" + json_format ;

        //-------------------------------------------//
        // TODO: This only to configure if is needed it
        // -------------------------------------------//
        loading_button_icon(chart_location + '_update_button');
        //------------------------------------------//

        queue()
            .defer(d3.json,data_url)
            .await(update_now);
        function update_now(error,new_data){
            if(new_data == null){
                console.log(error);
            }
            make_list(error,new_data);
        }
    }
}
//---------------------------------------------------------------
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