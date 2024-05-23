const nodes2ts = require('nodes2ts');

/**
 * Returns all S2 cell IDs within a specified distance from a coordinate pair or an S2 cell ID.
 * 
 * @param {number|string} input - Latitude and longitude as a string separated by comma or an S2 cell ID.
 * @param {number} distance - Distance in meters.
 * @param {number} level - The S2 level at which the cells should be enumerated.
 * @returns {Array} An array of S2 cell IDs.
 */
function findS2Cells(input, distance, level) {
    let region;
    if (typeof input === 'string' && input.includes(',')) {
        const [latitude, longitude] = input.split(',').map(Number);
        const latLng = nodes2ts.S2LatLng.fromDegrees(latitude, longitude);
        const cap = nodes2ts.S2Cap.fromAxisAngle(latLng.toPoint(), nodes2ts.S1Angle.radians(distance / 6371000)); // Earth's radius in meters
        region = new nodes2ts.S2RegionCoverer();
        region.minLevel = level;
        region.maxLevel = level;
        region.maxCells = 500; // You can adjust this for performance or more granularity
        return region.getCoveringCells(cap);
    } else {
        const cell = new nodes2ts.S2CellId(input);
        const cap = nodes2ts.S2Cap.fromAxisAngle(cell.toPoint(), nodes2ts.S1Angle.radians(distance / 6371000)); // Earth's radius in meters
        region = new nodes2ts.S2RegionCoverer();
        region.minLevel = level;
        region.maxLevel = level;
        region.maxCells = 500; // Adjust based on required granularity
        return region.getCoveringCells(cap);
    }
}

// Example usage
// const input = "56.292858, 8.173521"; // or an S2 cell ID as a string
const input = "0100011001001010001111111000101001100100101010000101000000000000"
const distance = 1000; // in meters
const level = 15;
const cells = findS2Cells(input, distance, level);
console.log(cells.map(cell => BigInt(cell.id)));
