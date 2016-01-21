define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.temperaturedetail = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear(),
            yValue = 0,
            css = '';

        var temperaturedetail = function (selection) {
            var linesegments;
           

//            console.log(selection);
            //selection.sort(function (d) {return d.date;});

//            console.log(Object.keys(selection));
        
            selection.each(function(ds) {
                

                //console.log(Object.keys(ds));
//                console.log(ds);
                d3.select('.detailTemp').selectAll('.linesegment').remove();
                linesegments = d3.select('.detailTemp').selectAll('.linesegment')
                    .data(Object.keys(ds)); 

                linesegments.enter().append('g')
                    .classed('linesegment', true)
                    .attr("id", function(d) { return 'linesegment'+d; })
                    .append('path');

                linesegments.exit().remove();
                
                Object.keys(ds).forEach(function(key) {
                    //console.log('key '+key);
                    var line = d3.svg.line()
                        .x(function(d) {return xScale(d.date); })
                        .y(function(d) {return yScale(d.temp); });

                    //console.log(data2);
                    var selector = "g#linesegment"+key+" > path";
                    //console.log(selector);
                    var p = d3.selectAll(selector)
                        .classed({
                         'detailTemp': true,
                         'detailLine': true,
                        })
                        .attr('d',line(ds[key]));


//                    segment.exit().remove();
                    //console.log(data2);
/*
                    var p = segment.append('path')
                        .attr('d',line(data2))
                        .classed({
                         'detailTemp': true,
                         'detailLine': true,
                        });
*/
                    //segment.exit().remove();
  //                  console.log(segment);
                    //segment.exit().remove();
/*                        .data(data)
                        
                        .attr('d', line);
*/
                    //segment.exit().remove();
//                    var val = dataseries[key];
//                    console.log(val);


                });
            });
        };

        temperaturedetail.xScale = function (value) {
            if (!arguments.length) {
                return xScale;
            }
            xScale = value;
            return temperaturedetail;
        };

        temperaturedetail.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return temperaturedetail;
        };
/*
        temperaturedetail.yValue = function (value) {
            if (!arguments.length) {
                return yValue;
            }
            yValue = value;
            return temperaturedetail;
        };


        temperaturedetail.css = function (value) {
            if (!arguments.length) {
                return css;
            }
            css = value;
            return temperaturedetail;
        };
*/
        return temperaturedetail;
    };
});