const nodes2ts = require('nodes2ts');

/**
 * Computes the S2 cell ID's binary representation at a specific S2 level for a given pair of latitude and longitude.
 * 
 * @param {number} latitude - The latitude of the point.
 * @param {number} longitude - The longitude of the point.
 * @param {number} level - The S2 level at which the cell ID should be calculated.
 * @returns {string} The 64-bit binary string representation of the S2 cell ID.
 */
function getS2BinaryID(latitude, longitude, level) {
    // Convert latitude and longitude to an S2 cell
    const point = nodes2ts.S2LatLng.fromDegrees(latitude, longitude).toPoint();

    // Get the S2 cell ID at the specified level
    const cellID = nodes2ts.S2CellId.fromPoint(point).parentL(level);

    // Convert the cell ID to a BigInt for full precision
    const decimalID = BigInt(cellID.id);

    console.log(cellID + " " + decimalID + " " + decimalID.toString(2).padStart(64, '0'));

    // Convert the decimal ID to a 64-bit binary string
    return decimalID.toString(2).padStart(64, '0');
}

// Example usage
const latitude = 56.292858;
const longitude = 8.173521;
const level = 24;
const binaryID = getS2BinaryID(latitude, longitude, level);

console.log(binaryID);
