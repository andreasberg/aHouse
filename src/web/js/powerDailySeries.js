define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.powerdaily = function () {

        var xScale = d3.time.scale(),
            yScale = d3.scale.linear();

        var powerdaily = function (selection) {

            var line = d3.svg.line();
            var rangeBand = _.floor(xScale(new Date(8.64e7)))-xScale(new Date(0));

            line.x(function (d) { return xScale(d.date)+rangeBand/2; });

            selection.each(function (data) {
        //        console.log(data);
                line.y(function (d) { return yScale(d.c8); });

                var path = d3.select(this).selectAll('.powerdaily')
                    .data([data]);

                path.enter().append('path');

                path.attr('d', line)
                    .classed('powerdaily', true);

                path.exit().remove();
            });

        };

        powerdaily.xScale = function (value) {
            if (!arguments.length) {
                return xScale;
            }
            xScale = value;
            return powerdaily;
        };

        powerdaily.yScale = function (value) {
            if (!arguments.length) {
                return yScale;
            }
            yScale = value;
            return powerdaily;
        };

        return powerdaily;
    };
});