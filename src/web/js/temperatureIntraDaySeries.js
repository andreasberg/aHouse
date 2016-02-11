define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.temperatureintraday = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear(),
            yValue = 0;

        var temperatureintraday = function (selection) {
            var linesegments;
           

//            console.log(selection);
            //selection.sort(function (d) {return d.date;});

//            console.log(Object.keys(selection));
        
            selection.each(function(ds) {
                

/*                console.log('draw');
                console.log(ds.keys());
*/
                //console.log(ds);

//                d3.select('.detailTemp').selectAll('.linesegment').remove();
                linesegments = d3.select('.detailTemp').selectAll('.linesegment')
                    .data(ds.keys()); 

                linesegments.enter().append('g')
                    .classed('linesegment', true)
                    .append('path');

                linesegments.attr("id", function(d) { return 'linesegment'+d; })

                linesegments.exit().remove();
                
                ds.forEach(function(d) {
                    //console.log('key '+key);
                    //console.log('linesegment'+d);
                    //console.log(this);
                    //console.log(this.get(d));
                    var line = d3.svg.line()
                        .x(function(d) {return xScale(d.date); })
                        .y(function(d) {/*console.log('d.temp: '+d.temp+' yScaled: '+yScale(d.temp));*/ return yScale(d.temp); });

                    //console.log(data2);
                    var selector = "g#linesegment"+d+" > path";
                    //console.log(selector);
                    var p = d3.selectAll(selector)
                        .classed({
                         'detailTemp': true,
                         'detailLine': true,
                        })
                        .attr('d',line(this.get(d)));




                });
            });
        };

        temperatureintraday.xScale = function (value) {
            if (!arguments.length) {
                return xScale;
            }
            xScale = value;
            return temperatureintraday;
        };

        temperatureintraday.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return temperatureintraday;
        };
/*
        temperatureintraday.yValue = function (value) {
            if (!arguments.length) {
                return yValue;
            }
            yValue = value;
            return temperatureintraday;
        };


        temperatureintraday.css = function (value) {
            if (!arguments.length) {
                return css;
            }
            css = value;
            return temperatureintraday;
        };
*/
        return temperatureintraday;
    };
});