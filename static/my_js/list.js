/**
 * Created by Roberto on 10/29/2016.
 * this is only for test the Json list data format
 * is not working because it has problem with indexes
 */

// Global variables:
var chart = dc.dataTable("#list"); // "#list" is referenced in the html file as <table id="list"></table>
var ndx;

/// Useful functions:
//// This function gets the parameter values from the URL
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

///////////////////////////////////////////////////////

var list_projection = getQueryVariable('list_projection');
var time_query = getQueryVariable('time_query');
var json_format = 'list';

// TODO: Put the default variables
// V022_vent02_CO2 is a default variable
// 2013-11-05 is a default time
if(list_projection == null){
    list_projection = 'timestamp,V022_vent02_CO2';
}
if(time_query == null){
    time_query = '2013-11';
}

var data_url = "/data" + "?list_projection=" + list_projection + "&time_query=" + time_query
     + "&json_format=" + json_format ;

queue()
    .defer(d3.json, data_url)
    .await(make_list);

function make_list(error,json_list) {
    var dateFormat = d3.time.format("%Y-%m-%d %H:%M:%S");
    //Transform the timestamp to be used in my_js
    json_list['timestamp'] = json_list['timestamp'].map(dateFormat.parse);

    // Get the list of attributes -> list of tag names
    var att_list = Object.keys(json_list);
    var tag_list = [];
    for(var idx in att_list)
    {   var attribute_value = att_list[idx];
        if( attribute_value !== "timestamp") {
           tag_list =  tag_list.concat(attribute_value);
        }
    }

    // Processing the crossfilter
    // Length of the array
    var N_size = json_list['timestamp'].length;
    var index = _.range(N_size);

    // Create the crossfilter
    ndx = crossfilter(index);
    //var fmt = d3.format('.2f');
    var tagDimension =
            ndx.dimension(function (d)  // here d refers to the index
                {
                    var values = [];
                    for(var x in tag_list){
                        values = values.concat(json_list[tag_list[x]][d]);
                    }
                    return values;}
            ),
        grouping = function (d) //here d refers to the index
        {
            //return [d["timestamp"].toLocaleDateString()];
           // return [d["timestamp"].toISOString()];
            return [json_list.timestamp[d].toDateString()];
        };
    // Creating the table_chart using the 3d library
    chart
        .width(600)
        .height(400)
        .dimension(tagDimension)
        .group(grouping)
        .size(Infinity)
        .columns(tag_list)
        .sortBy(function (d) {
            return [json_list.timestamp[d].toISOString()];
        })
        .order(d3.ascending);
    update();
    chart.render();
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

