define([
    'd3',
    'ab',
    'queue',
    'spin',
    'lodash',
    'navDataSeries',
    'temperatureBarSeries',
    'axisTemperatureBarSeries',
    'powerDailySeries',
    'powerStatsSeries',
    'temperatureIntraDaySeries',
    'barOverlaySeries',
    'trackerSeries'
], function (d3, ab) {
    'use strict';

    /* ripped from http://blog.scottlogic.com/2014/09/19/interactive.html */

    var queue = require('queue');
    var Spinner = require('spin');
    var _ = require('lodash');
    //var _ = require('underscore-min');

    var showDetailAtLessThanDays = 7; // showing detail data at scale (x-domain) less than x days 

    // Requesting data (asynch) 
//    var parseDate = d3.time.format("%d-%m-%yT%H:%M:%S").parse;
    var parseDate = d3.time.format("%d-%m-%y").parse;
    var parseDateYmd = d3.time.format("%Y%m%d").parse;
    var parseTimestamp = d3.time.format("%Y/%m/%d %H:%M:%S").parse;  //  e.g. 2015/01/01 00:25:29
    var parseTimestamp2 = d3.time.format("%Y-%m-%d %H:%M:%S").parse;  //  e.g. 2015/01/01 00:25:29
    var parseTimestampPower = d3.time.format("%Y-%m-%d %H:%M:%S.%L").parse;
    //var formatDate = d3.time.format("%Y-%m-%dT%H:%M:%S%Z");
    var formatDate = d3.time.format("%Y%m%d");

    var tempData = {};

    var dummyDetailTempEntry = [
        {
            date : new Date('1970-01-01'),
            temp : 20.0,
            hum : 100.0
        }
    ];

    var temperatureDetailData = d3.map();
    //var temperatureDetailData = d3.map({"19700101" : dummyDetailTempEntry});
    //temperatureDetailData.set('19700102',dummyTempEntry);

    var powerStatsData = {};


    var navXextent = 90 // length of navchart x scale in days

    var zoomLevels = [
        {   
            id: 0,
            min_H: 0,    // min of extent in hours
            max_H: 6,    // max of extent in hours
            extent_H: 6, // length of extent in hours
            period_M: 5   // period in minutes
        }
        ,{ id : 1, min_H : 6, max_H : 36, extent_H : 30, period_min : 30 }
        ,{ id : 2, min_H : 36, max_H : 300, extent_H : 264, period_min : 180 }
        ,{ id : 3, min_H : 300, max_H : 3360, extent_H : 3060, period_min : 30 }
        ,{ id : 5, min_H : 3360, max_H : null, extent_H : 3060, period_min : 30 }
    ];

    // Prepare spinner
    var spinopts = {
      lines: 17, // The number of lines to draw
      length: 15, // The length of each line
      width: 8, // The line thickness
      radius: 20, // The radius of the inner circle
      corners: 0.6,
      opacity: 0.05,
      //shadow: on,
      color: ['#2969a2','#3b83be','#57afea','#78c9f1','#92dcd3','#9adcab','#a2dd77','#cbdd5e','#ecdd52','#cbdd5e','#a2dd77','#9adcab','#92dcd3','#78c9f1','#57afea','#3b83be'], 
      speed: 0.6, // Rounds per second
      trail: 80, // Afterglow percentage
      className: 'spinner', // The CSS class to assign to the spinner
    };
    var spintarget = document.getElementById('chart'); // div.chart surrounding the main chart, width = 100%
    var spinner = new Spinner(spinopts).spin(spintarget);

    var dsv = d3.dsv(";", "text/plain");
    //Timestamp;ts;value;Humidity
//    var csv = d3.csv();
    //"ts","circuit9_cumul","circuit10_cumul","circuit11_cumul","circuit12_cumul","circuit13_cumul","circuit14_cumul","circuit15_cumul","circuit16_cumul"

    var preload_q = queue() // no limit to parallellism
//        .defer(dsv, "data/tempdata/tempoutside2015_average.csv", function(d) { return { date : parseDate(d.ts), temp_avg : +d.value , temp_min : +d.temp_min, temp_max : +d.temp_max, hum_avg : +d.hum_avg}; })
//      http://localhost:8888/clima?q=climaDaily&tz=Europe/Helsinki&datatype=Humidity
        .defer(d3.csv, "/clima?q=climaDaily&tz=Europe/Helsinki&datatype=Temperature", function(d) {
            return { 
                // d.ts = '20160106'
                date : parseDateYmd(d.ts), 
                temp_avg : +d.avg , 
                temp_min : +d.min, 
                temp_max : +d.max
            };
        })
/*        .defer(d3.csv, "data/powerDaily.csv", function(d) { 
            var ts = parseTimestampPower(d.ts.substring(0,d.ts.length-3)); // truncate microseconds from timestamp since JavaScript doesn't handle it well
            return { 
                date : new Date(ts.getFullYear(),ts.getMonth(),ts.getDate()), 
                sampleTimestamp : ts, 
                c1 : +d.circuit9_cumul,
                c2 : +d.circuit10_cumul,
                c3 : +d.circuit11_cumul,
                c4 : +d.circuit12_cumul,
                c5 : +d.circuit13_cumul,
                c6 : +d.circuit14_cumul,
                c7 : +d.circuit15_cumul,
                c8 : +d.circuit16_cumul
            }; 
        })
*/        .defer(d3.csv, "/power?q=powerstats", function(d) {
            return { 
                date : parseTimestamp2(d.ts), 
                c1_use : +d.c1_use, c1_cumul : +d.c1_cumul, c1_peak : +d.c1_peak,
                c2_use : +d.c2_use, c2_cumul : +d.c2_cumul, c2_peak : +d.c2_peak,
                c3_use : +d.c3_use, c3_cumul : +d.c3_cumul, c3_peak : +d.c3_peak,
                c4_use : +d.c4_use, c4_cumul : +d.c4_cumul, c4_peak : +d.c4_peak,
                c5_use : +d.c5_use, c5_cumul : +d.c5_cumul, c5_peak : +d.c5_peak,
                c6_use : +d.c6_use, c6_cumul : +d.c6_cumul, c6_peak : +d.c6_peak,
                c7_use : +d.c7_use, c7_cumul : +d.c7_cumul, c7_peak : +d.c7_peak,
                c8_use : +d.c8_use, c8_cumul : +d.c8_cumul, c8_peak : +d.c8_peak,
                all_use : +d.all_use, all_cumul : +d.all_cumul, all_peak : +d.all_peak,
                rowid : d.id,ts : d.ts
            }; 

        }); 
        

        //.defer(dsv, "utetemp/utetemp20150101.csv", function(d) { return { date : parseTimestamp(d.ts), temp : +d.value , hum : +d.Humidity}; });
        //.defer(dsv, "utetemp/utetemp20150102.csv", function(d) { return { date : parseTimestamp(d.ts), temp : +d.value , hum : +d.Humidity}; });

  
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // The primary chart
    // Set up the drawing area
    
    console.log(d3.select('#chart').style('width'));
    var margin = {top: 20, right: 35, bottom: 30, left: 35},
        width = parseInt(d3.select('#chart').style('width'), 10)-200,
        width = width - margin.left - margin.right,
        height = 500 - margin.top - margin.bottom;


    var plotChart = d3.select('#chart').classed('chart', true)
        .attr('align', 'center')
        .append('svg')
        .attr('width', width + margin.left + margin.right)
        .attr('height', height + margin.top + margin.bottom)
        .classed('plot',true)
        .append('g')
        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

    var plotArea = plotChart.append('g')
        .attr('clip-path', 'url(#plotAreaClip)');

    plotArea.append('clipPath')
        .attr('id', 'plotAreaClip')
        .append('rect')
        .attr({ width: width, height: height });

    // Scales

    var xScale = d3.time.scale(),
        yScale = d3.scale.linear(),
        yScalePower = d3.scale.linear();
        //yScalePower = d3.scale.log();   // log scale

    // Set scale ranges
    xScale.range([0, width]);
//    yScale.range([height, 0]);
    yScale.range([height-200, 0]);
//    yScalePower.range([height, 0]);
    yScalePower.range([height, height-200]);

    // Scale domain set later based on data

    // Prepare axes
    var xAxis = d3.svg.axis()
        .scale(xScale)
        .orient('bottom')
        .ticks(10);

    var yAxis = d3.svg.axis()
        .scale(yScale)
        .orient('left');

    var formatValue = d3.format(".2s");
    var yAxisPower = d3.svg.axis()
        .scale(yScalePower)
        .tickFormat(function (d) { return formatValue(d);})
        .orient('right');


    var yaxistempbar = ab.series.axistemperaturebar()
        .yScale(yScale);

    // Prepare temperature data series
    var tempdaily = ab.series.temperaturebar()
        .xScale(xScale)
        .yScale(yScale);

    // Prepare power consumption data series
    var powerdaily = ab.series.powerdaily()
        .xScale(xScale)
        .yScale(yScalePower);

    // Prepare power consumption statistics data series
    var powerstats = ab.series.powerstats()
        .xScale(xScale)
        .yScale(yScalePower);

    // Prepare temperature detail (intra-day, lazy-loaded) series
    var detailTemp = ab.series.temperatureintraday()
        .xScale(xScale)
        .yScale(yScale);

    // Prepare baroverlay
    var baroverlay = ab.series.baroverlay()
        .xScale(xScale)
        .yScale(yScale);

    // Prepare tracker
    var tempdailytracker = ab.series.tracker()
        .xScale(xScale)
        .yScale(yScale)
        .yValue('temp_avg')
//        .movingAverage(5)
        .css('tracker-avgtemp-movavg');

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Prepare navigation chart

    var navWidth = width,
        navHeight = 100 - margin.top - margin.bottom;

    // Set up the drawing area

    var navChart = d3.select('#chart').classed('chart', true).append('svg')
        .classed('navigator', true)
        .attr('width', navWidth + margin.left + margin.right)
        .attr('height', navHeight + margin.top + margin.bottom)
        .append('g')
        .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');


    // Prepare navchart clippath
    var navArea = navChart.append('g')
        .attr('clip-path', 'url(#navAreaClip)');

    navArea.append('clipPath')
        .attr('id', 'navAreaClip')
        .append('rect')
        .attr({ width: navWidth, height: navHeight });

    // Prepare scales
    var navXScale = d3.time.scale()
            .range([0, navWidth]),
        navYScale = d3.scale.linear()
            .range([navHeight, 0]);

    // Prepare axes
    var navXAxis = d3.svg.axis()
        .scale(navXScale)
        .orient('bottom');

    var navigation = ab.series.navigation()
        .xScale(navXScale)
        .yScale(navYScale);

    var navSeries = navArea.append('g')
        .attr('class', 'navigation');


    // Prepare temperature daily data series
    var tempDailySeries = plotArea.append('g')
        .attr('class', 'tempdaily');

    // Prepare power daily data series
    var powerDailySeries = plotArea.append('g')
        .attr('class', 'powerdaily');

    // Prepare power stats data series
    var powerStatsSeries = plotArea.append('g')
        .attr('class', 'powerstats');

    // Prepare baroverlay in data series
    var baroverlaySeries = plotArea.append('g')
        .attr('class', 'baroverlay')

    // Prepare data series
    var detailSeries = plotArea.append('g')
        .attr('class', 'detailTemp');

    // Prepare tracker series
    var tempDailyTrackerSeries = plotArea.append('g')
        .attr('class', 'tempdailytracker');



    var yAxisTempBarSeries = plotChart.append('g')
        .attr('class', 'y_bar axis');

    var minDate,maxDate,yMin,yMax;
    var viewport,zoom,overlay; 

    
    function plotAxes(data) {
        minDate = d3.min(data, function (d) { return d.date; }).getTime();
        //maxDate = new Date(d3.max(data, function (d) { return d.date; }).getTime() + 8.64e7);
        maxDate = new Date(); // now as maxDate
        yMin = d3.min(data, function (d) { return d.temp_min; });
        yMax = d3.max(data, function (d) { return d.temp_max; });

        // Set scale domains
        xScale.domain([minDate, maxDate]);
        yScale.domain([yMin, yMax]).nice();

        // Plot axes
        plotChart.append('g')
            .attr('class', 'x axis')
            .attr('transform', 'translate(0,' + height + ')')
            .call(xAxis);

        plotChart.append('g')
            .attr('class', 'y axis')
            .call(yAxis);

    } 

    function plotAxisPower(data1,data2) {
        yMin = 0;
/*        yMax = _.max([
            d3.max(data1, function (d) { return _.max([d.c1,d.c2,d.c3,d.c4,d.c5,d.c6,d.c7,d.c8]); }),
            d3.max(data2, function (d) { return d.all_cumul; })
            ]);
*/
//        yMax = d3.max(data2,function (d) { return d.all_use; });
        yMax = 1000;
        // Set scale domains
        yScalePower.domain([yMin, yMax]).nice();
        //yScalePower.domain([1, yMax]).nice();   // log scale

        plotChart.append('g')
            .attr('class', 'y_power axis')
            .attr('transform', 'translate('+ width +',0)')
            .call(yAxisPower);

    } 


    function fillMissingDays(data,extent,fillerData,useLastObject) {
        var period = d3.time.day;
        var timeScale = d3.time.scale();
        timeScale.domain(extent).ticks(period);
        var newData = timeScale.ticks(period)
               .map(function(bucket) {
                    var foundObj = _.find(data,{ date: bucket });
                    var retObj;
                    if (foundObj) {
                        retObj = foundObj;
                        if (useLastObject === true) { // update filler with object found 
                            fillerData = foundObj;
                        }
                    } else {
                        retObj = _.clone(fillerData);
                        retObj.date = bucket;
                        retObj.exception = 'nodata'; // add exception flag indicating false data
                    }
                    return retObj;
                });
//        console.log(newData);
        return newData;
    }

    function fillMissingHoursPowerStats(data,extent,fillerData,useLastObject) {
        var period = d3.time.day;
        var timeScale = d3.time.scale();
        timeScale.domain(extent).ticks(period);
        var previousObject = {};
        var newData = timeScale.ticks(period)
               .map(function(bucket) {
                    var foundObj = _.find(data,{ date: bucket });
                    var retObj;
                    if (foundObj) {
                        retObj = foundObj;
/*                        if (useLastObject === true) { // update filler with object found 
                            fillerData = foundObj;
                        }
*/
                    if (useLastObject === true) { // update filler with object found 
                            fillerData = foundObj;
                        }
                    } else {
                        retObj = _.clone(fillerData);
                        retObj.date = bucket;
                        retObj.exception = 'nodata'; // add exception flag indicating false data
                    }
                    previousObject = retObj; 
                    return retObj;
                });
//        console.log(newData);
        return newData;
    }


    // Await all (preload) data before continuing 
    preload_q.awaitAll(function(error, results) { 
        //console.log(results);
        // results[0] = temperature data (daily)
        // results[1] = power data (daily)

        var fullXextent = d3.extent(results[0], function(d) { return d.date; }); // get full domain 

        var tempDataFiller = {  
                temp_avg : 0.0,
                temp_min : 0.0,
                temp_max : 0.0,
                hum_avg : 100.0
            };

        tempData = fillMissingDays(results[0],fullXextent,tempDataFiller,true); // 

/*        var powerDataFiller = {  
                c1 : 0,
                c2 : 0,
                c3 : 0,
                c4 : 0,
                c5 : 0,
                c6 : 0,
                c7 : 0,
                c8 : 0
            };
*/        
        //var powerdata = fillMissingDays(results[1],fullXextent,powerDataFiller,true);

        var powerStatsFiller = {  
                all_cumul : 0,
                all_peak : 0,
                all_use : 0
            };

        powerStatsData = fillMissingHoursPowerStats(results[1],fullXextent,powerStatsFiller,true);
        console.log(powerStatsData);

        // Draw axes based on initial data
        plotAxes(tempData);
        plotAxisPower(powerStatsData);

        // Plot navigation chart
        plotNavChart(tempData);

        // Assign data to series that are drawn from start (lazy loaded data assign later)
        tempDailySeries.datum(tempData);
        //powerDailySeries.datum(powerdata);
        powerStatsSeries.datum(powerStatsData);
        tempDailyTrackerSeries.datum(tempData);    

        var temprange = _.range(yScale.domain()[0],yScale.domain()[1]+1);
        yAxisTempBarSeries.datum(temprange);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Navchart
        navXScale.domain([
                    new Date(maxDate.getTime() - (navXextent*8.64e7)), 
                    new Date(maxDate.getTime() + 8.64e7)
                ]);
        //console.log(navXScale.domain());
        navYScale.domain([-30, 30]);

        // Navchart axes
        navChart.append('g')
            .attr('class', 'x axis')
            .attr('transform', 'translate(0,' + navHeight + ')')
            .call(navXAxis);

        // Data series
        navSeries.datum(tempData);


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Zooming and panning

        zoom = d3.behavior.zoom()
            .x(xScale)
            .on('zoom', onZoom);

        overlay = d3.svg.area()
            .x(function (d) { return xScale(d.date); })
            .y0(0)
            .y1(height);

        plotArea.append('path')
            .attr('class', 'overlay')
            .attr('d', overlay(tempData))
    	    .call(zoom);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Resizing
        d3.select(window).on('resize', reSize); 


        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Setup

        var daysShown = 14;

        xScale.domain([new Date(maxDate - daysShown*8.64e7),maxDate]);

        spinner.stop();

        redrawChart();
        //redrawNavChart();
        panAndDrawNavChart();
        //updateViewport();
        updateZoomFromChart();



    });   //await



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Helper methods

    function reSize() {
        
        var theChart = d3.select('#chart');
        // update width
        width = parseInt(theChart.style('width'), 10) - 200;
        width = width - margin.left - margin.right;
        navWidth = width;

        console.log('resizing')
        console.log(width);
        // resize the chart
        xScale.range([0, width]);
        navXScale.range([0, navWidth]);
/*
        yScale.range([height,0]);
        yScalePower.range([height,0]);
*/
        //plotChart.attr('width', (width + margin.left + margin.right));
        //navChart.attr('width', (width + margin.left + margin.right));

        theChart.select('.plot').attr('width', width + margin.left + margin.right);
        theChart.select('.navigator').attr('width', navWidth + margin.left + margin.right);

        theChart.select('#plotAreaClip > rect').attr('width', width );
        theChart.select('#navAreaClip > rect').attr('width', navWidth);
        theChart.select('.y_power.axis').attr('transform', 'translate('+ width +',0)')

        xAxis.scale(xScale);
        navXAxis.scale(navXScale);
/*
        yAxis.scale(yScale);
        yAxisPower.scale(yScalePower);
*/
        theChart.select('.plot').select('.y_power.axis').call(yAxisPower);
        theChart.select('.plot').select('.y.axis').call(yAxis);
        theChart.select('.plot').select('.x.axis').call(xAxis);

        redrawChart();
        panAndDrawNavChart();
        //updateViewport();
        updateZoomFromChart();

    }

    function plotNavChart(data) {

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Viewport

        viewport = d3.svg.brush()
            .x(navXScale)
            .on("brush", function () {   
                //console.log('brushing');
                //console.log(navXScale.domain());             
                xScale.domain(viewport.empty() ? navXScale.domain() : viewport.extent());
                redrawChart();
//                panNavChart();
                if (!(viewport.empty()) && (
                    viewport.extent()[0] <= navXScale.domain()[0] || 
                    viewport.extent()[1] >= navXScale.domain()[1])) { 
                    panAndDrawNavChart(); 
                }
            })
            .on("brushend", function () {
                updateZoomFromChart();
            });

        //console.log("creating viewport");console.log(viewport.empty());console.log(navXScale.domain());
        navArea.append("g")
            .attr("class", "viewport")
            .call(viewport)
            .selectAll("rect")
            .attr("height", navHeight);

    }

    function onZoom() {

        maxDate = new Date();  // update maxDate to current time 
        // stop zooming past minDate and maxDate
        if (xScale.domain()[0] < minDate) {
            zoom.translate([zoom.translate()[0] - xScale(minDate) + xScale.range()[0], 0]);
        } else 
        if (xScale.domain()[1] > maxDate) {
            zoom.translate([zoom.translate()[0] - xScale(maxDate) + xScale.range()[1], 0]);
        }

        redrawChart();
        zoomNavChart();

    }

    function zoomNavChart() {

        // Two-level (extents: navXextent and [minDate,maxDate]) brushable nav chart

        if (xScale.domain()[1] - xScale.domain()[0] > navXextent * 864e5 / 2) { 
            // Hide viewport and show full date range in navChart when viewport 
            // extends longer than half the navchart (extent: navXextent) 
            // This nav chart also brushable, reload or zoom from main chart to reset to 
            // fixed length(extent: navXextent) navchart.
            viewport.clear();
            navChart.select('.viewport').call(viewport);
            navXScale.domain([minDate,maxDate]);
            navSeries.call(navigation); 
            navChart.select('.x.axis').call(navXAxis);
        }  else {
            if (viewport.empty()) {
                // set viewport based on current domain in main chart
                viewport.extent(xScale.domain());
                // adjust navchart (scale&graph) so that viewport is centered
                var midDateFromXScale = +xScale.domain()[0]+_.floor(((xScale.domain()[1]-xScale.domain()[0])/2));
                navXScale.domain([new Date(midDateFromXScale - navXextent*864e5/2),new Date(midDateFromXScale + navXextent*864e5/2)]);
                panAndDrawNavChart();
            } else {
                if(viewport.extent()[0] <= navXScale.domain()[0] || viewport.extent()[1] >= navXScale.domain()[1]) { 
                    panAndDrawNavChart(); 
                } else {
                    viewport.extent(xScale.domain());
                    navChart.select('.viewport').call(viewport);
                }
            }
        }
    }

    function updateViewport() {

        viewport.extent(xScale.domain());
        navChart.select('.viewport').call(viewport);
    }

    function panAndDrawNavChart() {

        var panSpeed = 0.5   // times the current viewport extent(in days roundedup) to jump when panning 
        if( viewport.extent()[0] > minDate && viewport.extent()[1] < maxDate) {
            var panTo;
            (viewport.extent()[0] <= navXScale.domain()[0]) ? panTo = 'left': (viewport.extent()[1] >= navXScale.domain()[1]) ? panTo = 'right' : null;

            if (panTo === 'left' || panTo === 'right') {
                var vp_days = _.ceil((viewport.extent()[1]-viewport.extent()[0])/864e5);
                var i; 
                (panTo === 'left') ? i = -1 : i = +1; // invert panning left
/*
                console.log('vp_days = '+vp_days);
                console.log('XSCALE.DOMAIN: '+xScale.domain()[0]+' - '+xScale.domain()[1] );
                console.log('NAVXSCALE.DOMAIN: '+navXScale.domain()[0]+' - '+navXScale.domain()[1] );
*/                
                var new_date = Date.parse((panTo === 'left') ? viewport.extent()[0] : viewport.extent()[1])+i*panSpeed*vp_days*864e5;
                var d0,d1;
                (panTo === 'left') ? d0 = (new_date>minDate) ? new_date : +minDate : d0 = (new_date<maxDate) ? new_date :+maxDate;
                d1 = d0 - i*navXextent*864e5;

                navXScale.domain([new Date(_.min([d0,d1])),new Date(_.max([d0,d1]))]);
            }            
        }
        navSeries.call(navigation); 
        navChart.select('.x.axis').call(navXAxis);
        updateViewport();
    }

    function updateZoomFromChart() {

        var fullDomain = maxDate - minDate,
            currentDomain = xScale.domain()[1] - xScale.domain()[0];

        var minScale = currentDomain / fullDomain,  // minimi zoom = maximal extent : show full domain, always recalculated based on current domain
            maxScale = currentDomain / 36e5;       // maximal zoom = minimal extent : show 1h (36e5 milliseconds)

        zoom.x(xScale)
            .scaleExtent([minScale, maxScale]);
    }


    function redrawChart() {

        var visibleDomain = xScale.domain()[1] - xScale.domain()[0];
        //console.log("Drawing X-domain "+xScale.domain()[0]+" to "+xScale.domain()[1]);
        var dataDomain = [
            new Date(xScale.domain()[0] - 8.64e7),
            new Date(xScale.domain()[1] + 8.64e7)
        ];
        var tempData_slice = _.partition(tempData,function(d) { return (d.date > dataDomain[0] && d.date < dataDomain[1]); });
        //console.log(tempData_slice);
        var temp_avg_min = d3.min(tempData_slice[0], function(d) { return d.temp_avg; }),
            temp_avg_max = d3.max(tempData_slice[0], function(d) { return d.temp_avg; });

        yScale.domain([
            temp_avg_min === undefined ? -10 : _.min([35, d3.min(tempData_slice[0], function(d) { return d.temp_min; })]),
            temp_avg_max === undefined ? 20 : _.max([-25, d3.max(tempData_slice[0], function(d) { return d.temp_max; })])
        ]).nice();

        var temprange = _.range(yScale.domain()[0],yScale.domain()[1]);
        yAxisTempBarSeries.datum(temprange);

        if (visibleDomain < showDetailAtLessThanDays*8.64e7) { // less than X days visible 
            temperatureDetailData = loadAndDrawDetailData(dataDomain);
            baroverlaySeries.datum(tempData_slice[0]);
            baroverlaySeries.call(baroverlay);

            // Hide tracker
            d3.selectAll("path.tracker").style("opacity", 0); 
            // Show mini-y-axes in tempbars
            d3.selectAll('.bar-y-axis').attr('style','opacity:1;');

        } else {
            //console.log('Hiding detail');

            detailSeries.datum(d3.map());  // call with empty dataset, e.g. remove
            detailSeries.call(detailTemp);
            baroverlaySeries.datum([]);
            baroverlaySeries.call(baroverlay);

            // Show tracker
            d3.selectAll("path.tracker").style("opacity", 1);
            // Hide mini-y-axes
            d3.selectAll('.bar-y-axis').attr('style','opacity:0;'); 
        }


        tempDailySeries.call(tempdaily);
        tempDailyTrackerSeries.call(tempdailytracker);

        //powerDailySeries.call(powerdaily);
        var powerStats_slice = _.partition(powerStatsData,function(d) { return (d.date > xScale.domain()[0] && d.date < xScale.domain()[1]); });
        //console.log(psd_slice);
        powerStatsSeries.datum(powerStats_slice[0]);
        yScalePower.domain([0, d3.max(powerStats_slice[0], function(d) { return d.all_use; })]).nice();
        //yScalePower.domain([1, d3.max(powerStats_slice[0], function(d) { return d.all_use; })]).nice(); // log scale

        powerStatsSeries.call(powerstats);

        yAxisTempBarSeries.call(yaxistempbar);

        plotChart.select('.y_power.axis').call(yAxisPower);
        plotChart.select('.y.axis').call(yAxis);
        plotChart.select('.x.axis').call(xAxis);



    }
 

    function loadAndDrawDetailData(dataDomain) {
        // Asynchronously loading detailed data (when needed) and draws the series

        var spinner = new Spinner(spinopts).spin(spintarget); // Start spinner

        var loadDates = d3.time.days(dataDomain[0], dataDomain[1]);

        var lazyload_q = queue(5); // limit 5 reqs in parallell
        // http://localhost:8888/clima?q=climaEvents&tz=Europe/Helsinki&mindate=160123&maxdate=160123&datatypes=Humidity
        for (var i = 0, len = loadDates.length; i < len; i++) {
            var d = formatDate(new Date(loadDates[i]));
            if (!temperatureDetailData.has(d)) { 
                var filename = "/clima?q=climaEvents&tz=Europe/Helsinki&datatypes=Temperature,Humidity&mindate="+d+"&maxdate="+d;
                //console.log('filename ='+filename);
                var pTsUTC = d3.time.format.utc("%Y-%m-%dT%H:%M:%S.%L").parse;  //  e.g. 2015-01-01T00:25:29.000
                lazyload_q.defer(d3.csv, filename, function(d) { 
                    return { 
                        date : pTsUTC(d.ts.substring(0,d.ts.length-4)),   // '2016-01-22T22:05:04.000000Z' -> '2016-01-22T22:05:04.000' truncate microseconds(and Z) from utc timestamp since JavaScript doesn't handle it well
                        temp : +d.Temperature, 
                        hum : +d.Humidity
                    }; 
                });
            } 
        }
        lazyload_q.awaitAll(function(error, results) { 
            if (error) throw error;
            for (var i = 0, len = results.length; i < len; i++) {
                if (!(typeof results[i][0] === 'undefined')) {
                    var dd = formatDate(new Date(results[i][0].date));
                    temperatureDetailData.set(dd,results[i]);
                }
            }
            //console.log(temperatureDetailData)
            var parseYmd = d3.time.format("%Y%m%d").parse
            var detailData_slice = _.partition(temperatureDetailData.entries(),function(d) { return (parseYmd(d.key) > dataDomain[0] && parseYmd(d.key) < dataDomain[1]); }); 
            var detailData_slice_map = d3.map(); // TODO: Create partitioning function for d3.map directly instead of this 2-step rebuild of subset map
            detailData_slice[0].forEach(function(d) { detailData_slice_map.set(d.key,d.value);});

            detailSeries.datum(detailData_slice_map);
            detailSeries.call(detailTemp);
            /*
            setTimeout(function() {
                spinner.stop();
            }, 1500);*/
            spinner.stop(); // Stop spinner
        });
        
        return temperatureDetailData;
    }
});