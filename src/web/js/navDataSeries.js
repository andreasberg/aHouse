define ([
    'd3',
    'ab'
], function (d3, ab) {
    'use strict';

    ab.series.navigation = function () {

        var navXScale = d3.time.scale(),
            navYScale = d3.scale.linear(),
            navHeight = 50;

        var navigation = function (selection) {
  
            var series;

//            console.log(selection);
        
            selection.each(function(data) {

/*                console.log('drawing navseries');
                console.log(navXScale.domain());
                console.log(navXScale.range());
*/                //console.log(data);

                d3.selectAll('g.navigation > path').remove();
                var navData = d3.svg.area()
                    .x(function (d) { return navXScale(Date.parse(d.date)); })
                    .y0(navHeight)
                    .y1(function (d) { return navYScale(d.temp_avg); });

                var navLine = d3.svg.line()
                    .x(function (d) { return navXScale(d.date); })
                    .y(function (d) { return navYScale(d.temp_avg); });
                series = d3.select(this);

                var navdata = series.selectAll('.navdata')
                    .data(data, function(d) { 
                        return [d];
                    });

                navdata.enter().append('path')
                    .attr('class', 'navdata')
                    .attr('d', navData(data));
                navdata.enter().append('path')
                    .attr('class', 'navline')
                    .attr('d', navLine(data));

                navdata.exit().remove();

            });
        };

        navigation.xScale = function (value) {
            if (!arguments.length) {
                return navXScale;
            }
            navXScale = value;
            return navigation;
        };

        navigation.yScale = function (value) {
            if (!arguments.length) {
                return navYScale;
            }
            navYScale = value;
            return navigation;
        };
        return navigation;
    };
});