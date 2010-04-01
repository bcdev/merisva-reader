/*
 * $Id: MVAHdf5Band.java,v 1.6 2003-08-18 12:56:15 tom Exp $
 *
 * Copyright (C) 2002 by Brockmann Consult (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation. This program is distributed in the hope it will
 * be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package org.esa.beam.dataio.merisva;

import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import org.esa.beam.framework.dataio.ProductIOException;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;

class MerisVaBand {

    private int _datasetID;
    private int _dataspaceID;
    private int _dataTypeID;
    private int _memDataspaceID;
    private long[] _start;
    private long[] _memStart;
    private long[] _count;
    private long[] _stride;
    private long[] _block;
    private int _lineWidth;
    private int _pDataType;
    private Object _line;

    /**
     * Constructs the object with default values.
     */
    MerisVaBand() {
        _datasetID = -1;
        _dataspaceID = -1;
        _dataTypeID = -1;
        _memDataspaceID = -1;
    }

    /**
     * Initializes the object with given daset and dataspace id.
     *
     * @param datasetId
     * @param dataspaceID
     */
    void init(int datasetId, int dataspaceID, int dataTypeID) throws ProductIOException {
        _datasetID = datasetId;
        _dataspaceID = dataspaceID;
        _dataTypeID = dataTypeID;

        _start = new long[2];
        _memStart = new long[2];
        _memStart[0] = 0;
        _memStart[1] = 0;

        _count = new long[2];
        _count[0] = 1;

        _stride = new long[2];
        _stride[0] = 1;
        _block = new long[2];
        _block[0] = 1;
        _block[1] = 1;

        try {
            long[] size = new long[2];
            long[] maxSize = new long[2];

            H5.H5Sget_simple_extent_dims(dataspaceID, size, maxSize);
            _lineWidth = (int) maxSize[1];
            _pDataType = H5Utils.convertHdfToProductDataType(_dataTypeID);
            createLineBuffer();

            _memDataspaceID = H5.H5Screate_simple(2, new long[]{1, _lineWidth}, new long[]{1, _lineWidth});
        } catch (HDF5Exception e) {
            throw new ProductIOException(e.getMessage());
        }
    }


    /**
     * Reads one line of geophysical data from the band stored on the diskfile.
     *
     * @param sourceMinX   the minimum X offset in source raster co-ordinates
     * @param sourceMaxX   the maximum X offset in source raster co-ordinates
     * @param sourceStepX  the sub-sampling in X direction within the region providing samples to be read
     * @param sourceY      the Y-offset (zero-based line index) in source raster co-ordinates
     * @param destBuffer   the destination raster which receives the sample values to be read
     * @param destArrayPos the current line offset within the destination raster
     */
    void readRasterLine(int sourceMinX, int sourceMaxX, int sourceStepX,
                        int sourceY, ProductData destBuffer, int destArrayPos) throws IOException {
        _start[0] = sourceY;
        _start[1] = sourceMinX;
        _stride[1] = sourceStepX;
        _count[1] = sourceMaxX - sourceMinX + 1;

        try {
            H5.H5Sselect_hyperslab(_memDataspaceID, HDF5Constants.H5S_SELECT_SET, _memStart, _stride, _count, null);
            H5.H5Sselect_hyperslab(_dataspaceID, HDF5Constants.H5S_SELECT_SET, _start, _stride, _count, null);
            H5.H5Dread(_datasetID, _dataTypeID, _memDataspaceID, _dataspaceID, HDF5Constants.H5P_DEFAULT, _line);
            copyData(destBuffer, destArrayPos, (int) _count[1]);
        } catch (HDF5Exception e) {
            throw new ProductIOException(e.getMessage());
        } finally {
        }
    }

    /**
     * Closes the band.
     */
    void close() throws IOException {
        if (_dataTypeID >= 0) {
            try {
                H5.H5Tclose(_dataTypeID);
                _dataTypeID = -1;
            } catch (HDF5LibraryException e) {
                throw new ProductIOException(e.getMessage());
            }
        }

        if (_dataspaceID >= 0) {
            try {
                H5.H5Sclose(_dataspaceID);
                _dataspaceID = -1;
            } catch (HDF5LibraryException e) {
                throw new ProductIOException(e.getMessage());
            }
        }

        if (_datasetID >= 0) {
            try {
                H5.H5Dclose(_datasetID);
                _datasetID = -1;
            } catch (HDF5LibraryException e) {
                throw new ProductIOException(e.getMessage());
            }
        }
    }

    /**
     * Retrieves the dataset identifier for the band.
     *
     * @return the dataset ID
     */
    int getDatasetID() {
        return _datasetID;
    }
    ///////////////////////////////////////////////////////////////////////////
    // END OF PACKAGE-ACCESS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Creates a line buffer according to the datatype of the dataspace stored.
     */
    private void createLineBuffer() {
        if (_pDataType == ProductData.TYPE_FLOAT32) {
            _line = new float[_lineWidth];
        } else if (_pDataType == ProductData.TYPE_FLOAT64) {
            _line = new double[_lineWidth];
        } else if ((_pDataType == ProductData.TYPE_INT8) || (_pDataType == ProductData.TYPE_UINT8)) {
            _line = new byte[_lineWidth];
        } else if ((_pDataType == ProductData.TYPE_INT16) || (_pDataType == ProductData.TYPE_UINT16)) {
            _line = new short[_lineWidth];
        } else if ((_pDataType == ProductData.TYPE_INT32) || (_pDataType == ProductData.TYPE_UINT32)) {
            _line = new int[_lineWidth];
        }
    }

    /**
     * Copies the data from the line read from file to the ProductData buffer.
     *
     * @param destBuffer   the data buffer
     * @param destArrayPos the offset in samples to the data buffer start
     */
    private void copyData(ProductData destBuffer, int destArrayPos, int elemCount) {
        if (_pDataType == ProductData.TYPE_FLOAT32) {
            float[] dest = (float[]) destBuffer.getElems();
            float[] source = (float[]) _line;
            for (int n = 0; n < elemCount; n++) {
                dest[n + destArrayPos] = source[n];
            }
        } else if (_pDataType == ProductData.TYPE_FLOAT64) {
            double[] dest = (double[]) destBuffer.getElems();
            double[] source = (double[]) _line;
            for (int n = 0; n < elemCount; n++) {
                dest[n + destArrayPos] = source[n];
            }
        } else if ((_pDataType == ProductData.TYPE_INT8) || (_pDataType == ProductData.TYPE_UINT8)) {
            byte[] dest = (byte[]) destBuffer.getElems();
            byte[] source = (byte[]) _line;
            for (int n = 0; n < elemCount; n++) {
                dest[n + destArrayPos] = source[n];
            }
        } else if ((_pDataType == ProductData.TYPE_INT16) || (_pDataType == ProductData.TYPE_UINT16)) {
            short[] dest = (short[]) destBuffer.getElems();
            short[] source = (short[]) _line;
            for (int n = 0; n < elemCount; n++) {
                dest[n + destArrayPos] = source[n];
            }
        } else if ((_pDataType == ProductData.TYPE_INT32) || (_pDataType == ProductData.TYPE_UINT32)) {
            int[] dest = (int[]) destBuffer.getElems();
            int[] source = (int[]) _line;
            for (int n = 0; n < elemCount; n++) {
                dest[n + destArrayPos] = source[n];
            }
        }
    }
}
