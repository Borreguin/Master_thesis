/**
 * Created by Roberto on 11/3/2016.
 */
var tag_name;
var chart_place = 'par_cord';
var start_time;
var end_time;

//---------------------------------------------------------------
//This is general information to use in the Menu ////////////////////
// if they were already used, they are saved in the local User
// so we need only to recollect this data from the local machine
var new_list_tag = loadData('_list_tag'),
    new_selected_category_button = loadData('_selected_category_button'),
    new_tag_alias = loadData('_tag_alias'),
    new_json_categories = loadData('_json_categories'),
    new_tag_category = loadData('_tag_category');

if(new_list_tag != null && new_list_tag != 'undefined'){
    if(new_list_tag!= null) list_tag =  new_list_tag;
    if(new_selected_category_button!=null) selected_category_button = new_selected_category_button;
    if(new_tag_alias!=null) tag_alias = new_tag_alias;
    if(new_json_categories!=null) json_categories = new_json_categories;
    if(new_tag_category!=null) tag_category = new_tag_category;
}

// TODO: Put the default variables
// V022_vent02_CO2 is a default variable
// 2013-11-05 is a default time
if(list_tag == null || list_tag == 'undefined'){
    //if it was already defined and saved in the client's computer
    if(new_list_tag != null){
        list_projection = new_list_tag[0];
        list_tag = new_list_tag;
    }else{
        list_projection  = 'V022_vent02_CO2';
        list_tag = ['V022_vent02_CO2'];
    }
    tag_name = list_projection;
}
else{
    tag_name = list_tag[0];
    list_projection = tag_name;
}

if(start_time == null){
    start_time = '2013-01-01';
}
if(end_time == null){
    end_time = '2013-12-20';
}


var data_url = '/data_hour_day' + "?list_projection=" + list_projection + '&start_time=' + start_time.toISOString()
            + '&end_time=' + end_time.toISOString() ;

queue()
    .defer(d3.json, data_url)
    .await(make_coord);

function make_coord(error,json_list) {
    /*var data = [
        [0,-0,0,0,0,3 ],
        [1,-1,1,2,1,6 ],
        [4,-4,16,8,0.25,9]
    ];*/
    console.log(error);
    tag_name = list_tag[0];
    //plotting_button_icon( heat_chart_place + '_update_button');
    // getting 7 dimensions, 1 per each day
    var keys = json_list['keys'];

    // linear color scale
    var blue_to_brown = d3.scale.linear()
        .domain([0, 23])
        .range(["steelblue", "brown"])
        .interpolate(d3.interpolateLab);


    var pc = d3.parcoords()('#'+ chart_place)
        .data(json_list[tag_name])
        //.dimensions(axe_dimensions)
        //.commonScale()
        //.autoscale()
        .composite("darken")
        .color(function(d) { return blue_to_brown(d["Hour"]); })
        .render()
        .reorderable()
        .brushMode("1D-axes")  // enable brushing
        .interactive();  // command line mode;

    pc.render();
    //-------------------------------------------//
    // TODO: This only to configure if is needed it
    //-------------------------------------------//
    ready_button_icon( chart_place + '_update_button');
    var title = tag_category[tag_name] + " / " + tag_alias[tag_name];
    update_title_chart( chart_place + '_title', title);
    //------------------------------------------//
}

//-------------------- Used to modify the chart  ------
// The following variables come from the Menu script:
// This chart only takes one variable at time
function update_chart() {
    if(list_tag.length > 0){
        var tag = list_tag[0];
        var data_url = '/data_hour_day' + '?list_projection=' + tag + '&start_time=' + start_time.toISOString()
            + '&end_time=' + end_time.toISOString() ;
        //-------------------------------------------//
        // TODO: This only to configure if is needed it
        // -------------------------------------------//
        update_title_chart(chart_place + '_title','Loading');
        loading_button_icon( chart_place + '_update_button');
        //------------------------------------------//
        var s = '<div id="' + chart_place  + '" class="parcoords"></div>';
        var element = document.getElementById(chart_place);
        element.innerHTML = s;
        queue()
            .defer(d3.json,data_url)
            .await(update_now);
        function update_now(error,new_data){

            if(new_data == null){
                console.log(error);
            }
            make_coord(error,new_data);
        }
    }
}

//------- This function gets the parameter values from the URL
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