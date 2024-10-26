# -*- coding: utf-8 -*-
"""
Created on Tue Sep  5 13:47:54 2023

@author: timot
"""

import numpy as np
from math import pi
from scipy.ndimage import median_filter


# %% functionalize the quick background subtraction & intensity correction


def quick_bg_sub(image):
    """
    image_nobg = quick_bg_sub(image)

    Subtracts the median of a grayscale image from all pixels, then sets
    negative pixels to zero.

    Parameters
    ----------
    image : numpy array
        A grayscale image to process.  This was made for 2-D arrays, but should
        support n-D arrays.

    Returns
    -------
    image_nobg : numpy array of the same shape as "image"
        A copy of the input image with the median subtracted and negative
        values set to zero.

    """
    image_nobg = image - np.median(image.ravel())
    image_nobg[image_nobg < 0] = 0

    return image_nobg


def remove_hot_pixels(image, median_size=3, significance=3.0):
    """
    filtered_image = remove_hot_pixels(image, median_size=3, significance=3.0)

    Identifies and removes hot pixels by looking for pixels that are
    significantly different relative to the median value of their
    neighbors.  The size of the neighborhood is defined by "median_size"
    and "significantly different" is defined as "significance" times the
    standard deviation of the intensity values of the entire image.


    Parameters
    ----------
    image : numpy array
        Array representation of a grayscale image.  Only tested on 2D images.

    median_size : int, optional
        The size of the median filter, see scipy.ndimage.median_filter's "size"
        input for further details.  The default is 3.

    significance : float, optional
        Multiplier for the standard deviation used to define "significantly
        different". The default is 3.0.

    Returns
    -------
    filtered_img : numpy array
        A copy of the input image with hot pixel replaced by the median
        intensity of their neighborhood.

    """
    # sanatize inputs
    median_size = int(median_size)

    # calculate the standard deviation of the image
    img_std = np.std(image.ravel())

    # calculate the difference between the image and the median filtered image
    smoothed_img = median_filter(image, size=median_size)
    diff = image - smoothed_img

    # identify hot pixels
    hot_pixel_mask = diff >= significance * img_std

    # replace hot pixels with kernel median
    filtered_img = image.copy()
    filtered_img[hot_pixel_mask] = smoothed_img[hot_pixel_mask]

    return filtered_img


def preprocess_image(image, median_size=3, significance=3.0):
    """
    filtered_img = preprocess_image(image, median_size=3, significance=3.0)

    preprocess_image: Apply "quick_bg_sub" and "remove_hot_pixels" to the
    input image.  Exists to reduce code repetition.

    Parameters
    ----------
    image : numpy array
        Array representation of a grayscale image.  Only tested on 2D images.

    median_size : int, optional
        The size of the median filter, see scipy.ndimage.median_filter's "size"
        input for further details.  The default is 3.

    significance : float, optional
        Multiplier for the standard deviation used to define "significantly
        different". The default is 3.0.

    Returns
    -------
    filtered_img : numpy array
        A copy of the input image with the median intensity subtracted from all
        pixels, then hot pixel replaced by the median intensity of their
        neighborhood.
    """

    # run quick background subtraction
    image_nobg = quick_bg_sub(image)

    # filter hot pixels
    filtered_img = remove_hot_pixels(
        image_nobg, median_size=median_size, significance=significance
    )

    return filtered_img


def azimuthal_integration(img, det, cake_parms):
    """
    azimuthal_integration:
        Azimuthally integrate the image about the center (given by det['centers'])
    by binning the pixels.

    USAGE: integrated_I, tth_bin_edges, tth_bin_centers = azimuthal_integration(img, det, cake_parms)

    Parameters
    ----------
    img : 2D numpy array
        The image to integrate, represented as a numpy array

    det : dict
        Dictionary containing parameters describing the detector.  Relevant keys are:
            'pix_dim' : numpy array of length 2
                The size of the detector in units of pixels.

            'centers' : numpy array of length 2
                The position of the center on the diffraction rings relative to
                the geometric center of the detector.  ORDER IS [y, x], units
                are pixels.  There's a silly negative sign on the y offset I (TJHL)
                should probably fix at some point.

            'pix_size' : float
                The side length of a pixel in mm.

            'distance' : float
                The sample-detector distance, in mm.

    cake_parms : dict
        Dictionary containing parameters describing the integration (cake)
        parameters.  Relevant keys are:
            'tth_bin_size' : float
                The angular size of each two-theta bin, units are degrees.

            'max_tth' : float
                The maximum two-theta angle to integrate to, in degrees.


    Returns
    -------
    integrated_I : TYPE
        DESCRIPTION.

    tth_bin_edges : TYPE
        DESCRIPTION.

    tth_bin_centers : TYPE
        DESCRIPTION.

    """

    pix_dim = det["pix_dim"]
    # centers = det['centers'] + 0.5*pix_dim
    centers = 0.5 * pix_dim
    centers[1] += det["centers"][1]
    centers[0] += -det["centers"][0]  # the sign flip accounts for flipud
    tth_bin_size = cake_parms["tth_bin_size"]

    # convert image to a list
    #   flipud is needed because I set y=0 at the bottom of the image
    I_list = np.flipud(img).ravel()

    # calculate radius and two-theta
    x_vec = np.arange(pix_dim[1])
    y_vec = np.arange(pix_dim[0])

    X, Y = np.meshgrid(x_vec, y_vec)

    r_list = (
        np.sqrt((X.ravel() - centers[1]) ** 2 + (Y.ravel() - centers[0]) ** 2)
        * det["pix_size"]
    )
    tth_list = 180 / pi * np.arctan(r_list / det["distance"])

    # eta_list = 180/pi * np.arctan2(Y.ravel()-centers[0], X.ravel()-centers[1])

    # calculate the bin edges
    try:
        max_tth = cake_parms["max_tth"]
    except:
        max_tth = np.amax(tth_list)

    tth_bin_edges = np.arange(0, max_tth, tth_bin_size)
    num_tth_bins = tth_bin_edges.size - 1
    tth_bin_centers = tth_bin_edges[:num_tth_bins] + tth_bin_size / 2

    # ### DEBUG ###
    # print('Weights shape = ' + str(I_list.shape))
    # print('a shape = ' + str(tth_list.shape))
    # print('r_list shape = ' + str(r_list.shape))
    # #############

    (
        integrated_I,
        edges,
    ) = np.histogram(tth_list, bins=tth_bin_edges, weights=I_list)

    return integrated_I, tth_bin_edges, tth_bin_centers


def azimuthal_integration_corrected(
    image, min_tth, detector, cake_params, filter_input=False
):
    """
    I_list, r_list, tth_list = azimuthal_integration_corrected(image, min_tth, detector, cake_params, filter_input=False)

    Performs basic background subtraction and azimuthally integrates the given
    diffraction pattern, then corrects the integrated intensity for the area of
    each bin and sets the minimum integrated I to zero.

    Parameters
    ----------
    image : 2D numpy array
        Array representation of the 2D diffraction pattern to integrate

    min_tth : float
        The minimum two-theta value, will be added to cake_params at some point

    detector : dictionary
        A dictionary containing values describing the instrumentation, should be
        renamed to "instrument" or similar at some point.

    cake_params : dictionary
        Dictionary containing values describing the polar integration parameters.

    filter_input : boolean, Optional
        If True, performs basic background subtraction and hot pixel filtering
        on the input image.  If False, uses the image as-given.  Default is False.

    Returns
    -------
    I_list : array
        The integrated intensities

    r_list : array
        Radius of each integrated intensities

    tth_list : array
        Two-theta  of each integrated intensities

    """

    if filter_input:
        # remove the background
        image_nobg = quick_bg_sub(image)

        # filter hot pixels
        image_temp = remove_hot_pixels(image_nobg)
    else:
        image_temp = image

    # azimuthally integrate
    int_I, tth_be, tth_bc = azimuthal_integration(image_temp, detector, cake_params)

    min_tth_bin_ID = int(np.sum(tth_bc < min_tth))

    # correct for different area of each bin
    r_be = np.tan(tth_be * pi / 180) * detector["distance"]
    bin_area = pi * (r_be[1:] ** 2 - r_be[: r_be.size - 1] ** 2)
    r_bc = np.tan(tth_bc * pi / 180) * detector["distance"]
    I_cor = int_I / bin_area

    # remove low two-theta region where the correction producess large intensities
    I_list = I_cor[min_tth_bin_ID:]
    r_list = r_bc[min_tth_bin_ID:]
    tth_list = tth_bc[min_tth_bin_ID:]

    # subtract the minium integrated I from all points to remove a bit more bg
    I_list -= np.amin(I_list)

    return I_list, r_list, tth_list
