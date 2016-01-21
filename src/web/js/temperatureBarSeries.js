define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.temperaturebar = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear();

        var line = d3.svg.line()
            .x(function (d) {
                return d.x;
            })
            .y(function (d) {
                return d.y;
            });

        var temperaturebar = function (selection) {
            var rectangleWidth = Math.floor(xScale(new Date(8.64e7)))-xScale(new Date(0))-1;

            selection.each(function (data) {

                //console.log("drawing bars");

                var bar = d3.select(this).selectAll('.bar')
                    .data(data, function(d) { 
                        return d.date;
                    });

                var barEnter = bar.enter().append('g')
                    .classed({
                        'bar' : true,
                        'nodata' : function(d) { return d.exception?true:false;} 
                     })
                    .attr('class', function(d) { return d3.select(this).attr('class') + ' c' +  Math.round(d.temp_avg > 0 ? d.temp_max : d.temp_min); });  // bar background color based on temp_max when avg in plus degrees, temp_min when avg in minus degrees

                barEnter.append('rect');

                bar.exit().remove();

                // Draw rectangles
                bar.selectAll('rect')
                    .attr('x', function(d) {
                        return xScale(d.date);
                    })
                    .attr('y', function(d) {
                        return yScale(d.temp_max);
                    })
                    .attr('width', rectangleWidth )
                    .attr('height', function(d) {
                        return yScale(d.temp_min)-yScale(d.temp_max);
                    });


            });


        };

        temperaturebar.xScale = function (value) {
            if (!arguments.length) {
                return xScale;
            }
            xScale = value;
            return temperaturebar;
        };

        temperaturebar.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return temperaturebar;
        };

        return temperaturebar;
    };
});