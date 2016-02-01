define ([
    'd3',
//    'lodash',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.powerstats = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear();
            //yScale = d3.scale.log();  // log scale

        var powerstats = function (selection) {
            var series, avgline;
            //var rectangleWidth = 1.5;
            var rectangleWidth = _.floor(xScale(new Date(864e5)))-xScale(new Date(0))-1;

            selection.each(function (data) {

                //console.log("drawing bars");
                series = d3.select(this);

                //console.log(series);

                var bar = series.selectAll('.bar')
                    .data(data, function(d) { 
                        return d.date;
                    });

                var barEnter = bar.enter().append('g')
                    .classed({
                        'bar' : true,
                        'nodata' : function(d) { return d.exception?true:false;} 
                     });

                barEnter.append('rect');
                bar.exit().remove();

                // Draw rectangles
                bar.selectAll('rect')
                    .attr('x', function(d) {
                        return xScale(d.date);
                    })
                    .attr('y', function(d) {
                        return yScale(d.all_use > 0 ? d.all_use : 0);
//                        return yScale(d.all_use > 1 ? d.all_use : 1);   // log scale
                    })
                    .attr('width', rectangleWidth)
                    .attr('height', function(d) {
                        return Math.abs(yScale(0)-yScale(d.all_use));
//                        return Math.abs(yScale(0)-yScale(d.all_use));     // log scale
                    });


            });


        };        

        powerstats.xScale = function (value) {
            if (!arguments.length) {
                return xScale;
            }
            xScale = value;
            return powerstats;
        };

        powerstats.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return powerstats;
        };

        return powerstats;
    };
});