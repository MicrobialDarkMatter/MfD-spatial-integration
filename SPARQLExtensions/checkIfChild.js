/**
 * Checks if a given S2 cell ID is a child of another S2 cell ID.
 * 
 * @param {number} parent - The parent S2 cell ID.
 * @param {number} child - The child S2 cell ID.
 * @returns {boolean} True if the child is a child of the parent, false otherwise.
 */
function checkIfChild(parent, child) {
    let parentLevel = Math.floor((parent.lastIndexOf("1") - 3) / 2);

    let parentSignificantBits = 3 + 2 * parentLevel;
    let parentSubstring = parent.substring(0, parentSignificantBits);

    return child.substring(0, parentSignificantBits) === parentSubstring;
}
