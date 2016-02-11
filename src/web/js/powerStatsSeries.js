define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.powerstats = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear();
            //yScale = d3.scale.log();  // log scale

        var sidemargin = 5
        var powerstats = function (selection) {
            var series, avgline;
            //var rectangleWidth = 1.5;
            var rectangleHalfWidth = _.floor((xScale(new Date(36e5))-xScale(new Date(0)))/2);
            
            (rectangleHalfWidth > sidemargin) && (rectangleHalfWidth -= sidemargin)

            selection.each(function (data) {

//                console.log("drawing power bars");
                series = d3.select(this);

//                console.log(data);

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
                        return xScale(d.date)-rectangleHalfWidth;
                    })
                    .attr('y', function(d) {
//                        console.log('ps y0 ' + yScale(0))
//                        console.log('ps d.value = '+d.value+' yScale(d.value) = '+yScale(d.value > 0 ? d.value : 0))
                        return yScale(d.value > 0 ? d.value : 0);
//                        return yScale(d.all_use > 1 ? d.all_use : 1);   // log scale
                    })
                    .attr('width', rectangleHalfWidth*2)
                    .attr('height', function(d) {
                        return Math.abs(yScale(0)-yScale(d.value));
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