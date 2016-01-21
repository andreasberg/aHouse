define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.axistemperaturebar = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear(),
            miniScale = d3.scale.linear();


        var line = d3.svg.line()
            .x(function (d) {
                return d.x;
            })
            .y(function (d) {
                return d.y;
            });

  /*      var averageLine = function (bars,rectangleWidth) {
            
            var paths = bars.selectAll('.average-line').data(function (d) {
                return [d];
            });

            paths.enter().append('path');

            paths.classed('average-line', true)
                .attr('d', function (d) {
                    return line([
                        { x: xScale(d.date)-rectangleWidth/2, y: yScale(d.temp_avg) },
                        { x: xScale(d.date)+rectangleWidth/2, y: yScale(d.temp_avg) }
                    ]);
                });
        };*/

        var axistemperaturebar = function (selection) {
            var barHeight = Math.floor(yScale(0)-yScale(1))-1;
            var barWidth = 20;
            selection.each(function (data) {

                //console.log("drawing y axis temp bars");
/*
                var linesegments = d3.select('.detailTemp').selectAll('.linesegment')
                    .data(ds.keys()); 
*/  
                //var dr = _.range(yScale.domain()[0],yScale.domain()[1]+1);
                d3.selectAll('.y-axis-tempbar').remove();

                //console.log(data);
                var bar = d3.select(this).selectAll('.y-axis-tempbar')
                    .data(data);

                var barEnter = bar.enter().append('g')
                    .classed({
                        'y-axis-tempbar' : true,
                        'bar' : true
                     })
                    .attr('class', function(d) { return d3.select(this).attr('class') + ' c' +  _.floor(d); });

                barEnter.append('rect');

                bar.exit().remove();

                //var colorclass = 'c' +  Math.round(d.temp_avg);
                //bar
                    
                // Draw rectangles
                bar.selectAll('rect')
                    .attr('value', function(d) { return d;})
                    .attr('x', function(d) {
                        return -barWidth;
                    })
                    .attr('y', function(d) { 
                        return yScale(d)-barHeight;
                    })
                    .attr('width', barWidth )
                    .attr('height', barHeight );




            });


        };

        axistemperaturebar.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return axistemperaturebar;
        };

        return axistemperaturebar;
    };
});