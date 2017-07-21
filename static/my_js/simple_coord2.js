/**
 * Created by Roberto on 11/3/2016.
 */

var data2 = d3.range(0,2*Math.PI,Math.PI/40)
  .map(function(x) {
    return {
      "-x": -x,
      x: x,
      "sin(x)": Math.sin(x),
      "cos(x)": Math.cos(x),
      "atan(x)": Math.atan(x),
      "exp(x)": Math.exp(x),
      "square(x)": x*x,
      "sqrt(x)": Math.sqrt(x),
    };
  });

var pc2 = d3.parcoords()("#example");

pc2
  .data(data2)
  .color("#000")
  .alpha(0.2)
  .margin({ top: 24, left: 0, bottom: 12, right: 0 })
  .render()
  .reorderable();