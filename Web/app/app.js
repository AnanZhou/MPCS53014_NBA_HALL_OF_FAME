'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
require('dotenv').config()
const port = Number(process.argv[2]);
const hbase = require('hbase')

const url = new URL(process.argv[3]);

// Hbase client

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port ?? (url.protocol === 'https:' ? 443 : 80),
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});


/*
var hclient = hbase({
	host: '10.0.0.26',
	path:  '/',
	port: 8090, // http or https defaults
	protocol: 'http', // Don't want the colon
	encoding: 'latin1',
});
*/

// utility functions to process HBase data
function counterToNumber(c) {
	const buffer = Buffer.from(c, 'latin1');
	if (buffer.length !== 8) {
		console.error("Invalid buffer length for BIGINT column:", buffer.length);
		return 0; // Default value for missing or invalid data
	}
	return Number(buffer.readBigInt64BE());
}


function rowToMap(row) {
	var stats = {};
	row.forEach(function (item) {
		const column = item['column'];
		const value = item['$'];

		if (column === "details:current_total_points" && value) {
			// Only parse current_total_points as BIGINT
			stats[column] = counterToNumber(value);
		} else if (value) {
			// Handle other columns as strings
			stats[column] = value.toString('utf8'); // Convert to string
		} else {
			console.warn(`Missing or invalid value for column: ${column}`);
		}
	});
	return stats;
}



app.use(express.static('public'));
app.get('/player.html', function (req, res) {
	const playerName = req.query.player;

	// Fetch data from zhoua_player_summary_hbase
	hclient.table('zhoua_player_summary_hbase').row(playerName).get((summaryErr, summaryCells) => {
		if (summaryErr) {
			console.error("Error fetching player summary:", summaryErr);
			res.status(500).send("Error fetching player summary.");
			return;
		}

		if (!summaryCells || summaryCells.length === 0) {
			console.error("No summary data found for player", playerName);
			res.status(404).send("Player not found.");
			return;
		}

		const summaryData = rowToMap(summaryCells);
		console.log("Mapped Summary Data:", summaryData);

		/*const playerName = req.query.player;*/
		console.log(`Fetching details for playerName: ${playerName}`);

		// Define the seasons to query
		const seasons = [
			'2003-04', '2004-05', '2005-06', '2006-07',
			'2007-08', '2008-09', '2009-10', '2010-11',
			'2011-12', '2012-13', '2013-14', '2014-15',
			'2015-16', '2016-17', '2017-18', '2018-19',
			'2019-20', '2020-21', '2021-22', '2022-23',
			'2023-24'
		];


		const rows = new Array(seasons.length); // Pre-allocate an array with the same length as seasons
		let fetchCount = 0; // To track when all rows are fetched

		seasons.forEach((season, index) => {
			const rowKey = `${playerName}_${season}`; // Combine player name and season for the row key
			hclient.table('zhoua_player_detailed_stats_hbase').row(rowKey).get((err, cells) => {
				fetchCount++;
				if (err) {
					console.error(`Error fetching row ${rowKey}:`, err);
					rows[index] = null; // Preserve the index even if there's an error
				} else if (cells && cells.length > 0) {
					// Attach the season ID to the row data
					const mappedRow = rowToMap(cells);
					mappedRow["details:season_id"] = season; // Explicitly add season_id to the mapped row
					rows[index] = mappedRow; // Store the row with the attached season ID
					console.log(`Fetched row for season ${season}:`, mappedRow);
				} else {
					console.warn(`No data found for row ${rowKey}`);
					rows[index] = null; // Preserve the index even if no data is found
				}

				// Check if all seasons have been processed
				if (fetchCount === seasons.length) {
					if (rows.every(row => row === null)) { // If all rows are null
						res.send(`<h2>No detailed stats found for ${playerName}</h2>`);
					} else {
						// Filter out null rows and send the response
						const detailedData = rows.filter(row => row !== null); // Exclude null entries
						const template = filesystem.readFileSync('player.mustache').toString();
						const html = mustache.render(template, {
							name: playerName,
							summary: summaryData, // Add summary data
							details: detailedData
						});

						res.send(html);
					}
				}
			});
		});


	});
});


app.listen(port);
