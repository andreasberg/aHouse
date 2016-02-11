define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.energystats = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear(),
            yAlign = 'full',   // default 'full'-width, other: 'left' = half-width-to-left, 'right' = half-width-to-right 
            colors = ["#98abc5", "#8a89a6", "#7b6888", "#6b486b", "#a05d56", "#d0743c", "#ff8c00"]

        var sidemargin = 5

        var energystats = function (selection) {
            var series, avgline;
            //var rectangleWidth = 1.5;
            
            var rectangleHalfWidth = _.floor((xScale(new Date(36e5))-xScale(new Date(0)))/2);
            (rectangleHalfWidth > sidemargin) && (rectangleHalfWidth -= sidemargin)

            //rectangleHalfWidth = (yAlign ==='full' ? rectangleHalfWidth*2 : rectangleHalfWidth)

            var color = d3.scale.ordinal().range(colors);

            selection.each(function (data) {

                series = d3.select(this);

                var bar = series.selectAll('.bar')
                    .data(data, function(d) { 
                        return d.date;
                    });

                var barEnter = bar.enter().append('g')
                    .classed({
                        'bar' : true
                     })
                    .attr("date", function(d) { return d.date; });
                    

                bar.exit().remove();

                bar = series.selectAll('.bar')
                    .attr("transform", function(d) { return "translate(" + (xScale(d.date) - rectangleHalfWidth + ((yAlign === 'right')?rectangleHalfWidth:0))+ ",0)"; });

                if (data.length > 0) { 
                    color.domain(data[0].keys)  // set color domain to array on 'name':s from first data-element
                    //color.domain(data[0].values.map(function(d) { return d.name;}));  
                }
                // Draw stacked rectangles

                var rect = bar.selectAll('rect')
                    .data(function(d) { return d.values; });

                var rectEnter = rect.enter().append('rect')
                    .attr('value', function(d) { return d.value; })
                    .attr('fill', function(d) { return color(d.name); });

                rect.exit().remove()

                bar.selectAll('rect')
                    .attr('width', yAlign ==='full' ? rectangleHalfWidth*2 : rectangleHalfWidth)
                    .attr('y', function(d) {return yScale(d.stack); })
                    .attr('height', function(d) {return yScale(d.stack-d.value)- yScale(d.stack); });

            }); // selection.each


        };      //  var enerygystats

        energystats.xScale = function (value) {
            if (!arguments.length) {
                return xScale;
            }
            xScale = value;
            return energystats;
        };

        energystats.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return energystats;
        };

        energystats.yAlign = function (value) {
            if (!arguments.length) {
                return yAlign;
            }
            yAlign = value;
            return energystats;
        };

        energystats.colors = function (value) {
            if (!arguments.length) {
                return colors;
            }
            colors = value;
            return energystats;
        };

        return energystats;
    };
});