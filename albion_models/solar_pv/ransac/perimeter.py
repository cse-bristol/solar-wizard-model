"""perimeter_crofton from scikit-image 0.19.0"""
import numpy as np
from scipy import ndimage as ndi


def perimeter_crofton(image, directions=4):
    """Calculate total Crofton perimeter of all objects in binary image.

    Parameters
    ----------
    image : (N, M) ndarray
        2D image. If image is not binary, all values strictly greater than zero
        are considered as the object.
    directions : 2 or 4, optional
        Number of directions used to approximate the Crofton perimeter. By
        default, 4 is used: it should be more accurate than 2.
        Computation time is the same in both cases.

    Returns
    -------
    perimeter : float
        Total perimeter of all objects in binary image.

    Notes
    -----
    This measure is based on Crofton formula [1], which is a measure from
    integral geometry. It is defined for general curve length evaluation via
    a double integral along all directions. In a discrete
    space, 2 or 4 directions give a quite good approximation, 4 being more
    accurate than 2 for more complex shapes.

    Similar to :func:`~.measure.perimeter`, this function returns an
    approximation of the perimeter in continuous space.

    References
    ----------
    .. [1] https://en.wikipedia.org/wiki/Crofton_formula
    .. [2] S. Rivollier. Analyse d’image geometrique et morphometrique par
           diagrammes de forme et voisinages adaptatifs generaux. PhD thesis,
           2010.
           Ecole Nationale Superieure des Mines de Saint-Etienne.
           https://tel.archives-ouvertes.fr/tel-00560838

    Examples
    --------
    >>> from skimage import data, util
    >>> from skimage.measure import label
    >>> # coins image (binary)
    >>> img_coins = data.coins() > 110
    >>> # total perimeter of all objects in the image
    >>> perimeter_crofton(img_coins, directions=2)  # doctest: +ELLIPSIS
    8144.578...
    >>> perimeter_crofton(img_coins, directions=4)  # doctest: +ELLIPSIS
    7837.077...
    """
    if image.ndim != 2:
        raise NotImplementedError(
            '`perimeter_crofton` supports 2D images only')

    # as image could be a label image, transform it to binary image
    image = (image > 0).astype(np.uint8)
    image = np.pad(image, pad_width=1, mode='constant')
    XF = ndi.convolve(image, np.array([[0, 0, 0], [0, 1, 4], [0, 2, 8]]),
                      mode='constant', cval=0)

    h = np.bincount(XF.ravel(), minlength=16)

    # definition of the LUT
    if directions == 2:
        coefs = [0, np.pi / 2, 0, 0, 0, np.pi / 2, 0, 0,
                 np.pi / 2, np.pi, 0, 0, np.pi / 2, np.pi, 0, 0]
    else:
        coefs = [0, np.pi / 4 * (1 + 1 / (np.sqrt(2))),
                 np.pi / (4 * np.sqrt(2)),
                 np.pi / (2 * np.sqrt(2)), 0,
                 np.pi / 4 * (1 + 1 / (np.sqrt(2))),
                 0, np.pi / (4 * np.sqrt(2)), np.pi / 4, np.pi / 2,
                 np.pi / (4 * np.sqrt(2)), np.pi / (4 * np.sqrt(2)),
                 np.pi / 4, np.pi / 2, 0, 0]

    total_perimeter = coefs @ h
    return total_perimeter
