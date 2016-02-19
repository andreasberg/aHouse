define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.energystats = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear(),
            yAlign = 'full',   // default 'full'-width, other: 'left' = half-width-to-left, 'right' = half-width-to-right 
            colors = ['#737373','#F15A60','#7AC36A','#5A9BD4','#FAA75B','#9e67AB','#CE7058','#D77FB4'] // default colors

        var sidemargin = 5

        var energystats = function (selection) {
            var series, avgline;
            //var rectangleWidth = 1.5;
            
            var rectangleHalfWidth = _.floor((xScale(new Date(36e5))-xScale(new Date(0)))/2);
            (rectangleHalfWidth > sidemargin) && (rectangleHalfWidth -= sidemargin)

            //rectangleHalfWidth = (yAlign ==='full' ? rectangleHalfWidth*2 : rectangleHalfWidth)
            var width = xScale.range()[1]-xScale.range()[0];
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

                var keys = []
                if (data.length > 0) { 
                    keys = data[0].keys
                }
                color.domain(keys.map(function (d) { return d.name; }))  // set color domain to array on 'name':s from first data-element
                // add(or remove when keys = []) legend
                var legend = d3.select('.detail.legend').selectAll('.legendentry.'+yAlign)
                    .data(keys, function(d){return d.name;});
                
                var legendEnter = legend.enter().append('g')
                    .attr('class','legendentry '+yAlign);
                
                legendEnter.append('rect')
                    .attr("width", 18)
                    .attr("height", 18);

                legendEnter.append('text')
                    .attr('x',24)
                    .attr('y',9)
                    .attr("dy", ".35em")
                    .style("text-anchor", "begin");
                
                legend.exit().remove();

                //legend.attr("transform", function(d, i) { return "translate("+ i * 100 + ",0)"; });
                legend.attr("transform", function(d,i) { return "translate("+ ((yAlign === 'right' ? width/2 : 0) + (d.dispoffset-d.dispname.length)*10 + i*24) + ",0)"; });
                legend.selectAll('rect').style("fill", function(d) { return color(d.name);});
                legend.selectAll('text').text(function(d) { return d.dispname; });

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