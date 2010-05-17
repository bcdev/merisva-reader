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

    private int datasetID;
    private int dataspaceID;
    private int dataTypeID;
    private int memDataspaceID;
    private int lineWidth;
    private int pDataType;
    private Object line;

    /**
     * Constructs the object with default values.
     */
    MerisVaBand() {
        datasetID = -1;
        dataspaceID = -1;
        dataTypeID = -1;
        memDataspaceID = -1;
    }

    /*
     * Initializes the object with given daset and dataspace id.
     */

    void init(int datasetId, int dataspaceID, int dataTypeID) throws ProductIOException {
        datasetID = datasetId;
        this.dataspaceID = dataspaceID;
        this.dataTypeID = dataTypeID;

        try {
            long[] size = new long[2];
            long[] maxSize = new long[2];

            H5.H5Sget_simple_extent_dims(dataspaceID, size, maxSize);
            lineWidth = (int) maxSize[1];
            pDataType = H5Utils.convertHdfToProductDataType(this.dataTypeID);
            createLineBuffer();

            memDataspaceID = H5.H5Screate_simple(2, new long[]{1, lineWidth}, new long[]{1, lineWidth});
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
     *
     * @throws java.io.IOException if the data could not be read
     */
    synchronized void readRasterLine(int sourceMinX, int sourceMaxX, int sourceStepX,
                                     int sourceY, ProductData destBuffer, int destArrayPos) throws IOException {
        long[] start = new long[]{sourceY, sourceMinX};
        long[] stride = new long[]{1, sourceStepX};
        long[] count = new long[]{1, sourceMaxX - sourceMinX + 1};
        try {
            long[] memStart = new long[]{0, 0};
            H5.H5Sselect_hyperslab(memDataspaceID, HDF5Constants.H5S_SELECT_SET, memStart, stride, count, null);
            H5.H5Sselect_hyperslab(dataspaceID, HDF5Constants.H5S_SELECT_SET, start, stride, count, null);
            H5.H5Dread(datasetID, dataTypeID, memDataspaceID, dataspaceID, HDF5Constants.H5P_DEFAULT, line);
            copyData(destBuffer, destArrayPos, (int) count[1]);
        } catch (HDF5Exception e) {
            throw new ProductIOException(e.getMessage());
        }
    }

    /*
     * Closes the band.
     */

    void close() throws IOException {
        if (dataTypeID >= 0) {
            try {
                H5.H5Tclose(dataTypeID);
                dataTypeID = -1;
            } catch (HDF5LibraryException e) {
                throw new ProductIOException(e.getMessage());
            }
        }

        if (dataspaceID >= 0) {
            try {
                H5.H5Sclose(dataspaceID);
                dataspaceID = -1;
            } catch (HDF5LibraryException e) {
                throw new ProductIOException(e.getMessage());
            }
        }

        if (datasetID >= 0) {
            try {
                H5.H5Dclose(datasetID);
                datasetID = -1;
            } catch (HDF5LibraryException e) {
                throw new ProductIOException(e.getMessage());
            }
        }
        if (memDataspaceID >= 0) {
            try {
                H5.H5Sclose(memDataspaceID);
                memDataspaceID = -1;
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
        return datasetID;
    }
    ///////////////////////////////////////////////////////////////////////////
    // END OF PACKAGE-ACCESS
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Creates a line buffer according to the datatype of the dataspace stored.
     */
    private void createLineBuffer() {
        if (pDataType == ProductData.TYPE_FLOAT32) {
            line = new float[lineWidth];
        } else if (pDataType == ProductData.TYPE_FLOAT64) {
            line = new double[lineWidth];
        } else if ((pDataType == ProductData.TYPE_INT8) || (pDataType == ProductData.TYPE_UINT8)) {
            line = new byte[lineWidth];
        } else if ((pDataType == ProductData.TYPE_INT16) || (pDataType == ProductData.TYPE_UINT16)) {
            line = new short[lineWidth];
        } else if ((pDataType == ProductData.TYPE_INT32) || (pDataType == ProductData.TYPE_UINT32)) {
            line = new int[lineWidth];
        }
    }

    /*
     * Copies the data from the line read from file to the ProductData buffer.
     *
     * @param destBuffer   the data buffer
     * @param destArrayPos the offset in samples to the data buffer start
     */

    private void copyData(ProductData destBuffer, int destArrayPos, int elemCount) {
        if (pDataType == ProductData.TYPE_FLOAT32) {
            float[] dest = (float[]) destBuffer.getElems();
            float[] source = (float[]) line;
            System.arraycopy(source, 0, dest, destArrayPos, elemCount);
        } else if (pDataType == ProductData.TYPE_FLOAT64) {
            double[] dest = (double[]) destBuffer.getElems();
            double[] source = (double[]) line;
            System.arraycopy(source, 0, dest, destArrayPos, elemCount);
        } else if ((pDataType == ProductData.TYPE_INT8) || (pDataType == ProductData.TYPE_UINT8)) {
            byte[] dest = (byte[]) destBuffer.getElems();
            byte[] source = (byte[]) line;
            System.arraycopy(source, 0, dest, destArrayPos, elemCount);
        } else if ((pDataType == ProductData.TYPE_INT16) || (pDataType == ProductData.TYPE_UINT16)) {
            short[] dest = (short[]) destBuffer.getElems();
            short[] source = (short[]) line;
            System.arraycopy(source, 0, dest, destArrayPos, elemCount);
        } else if ((pDataType == ProductData.TYPE_INT32) || (pDataType == ProductData.TYPE_UINT32)) {
            int[] dest = (int[]) destBuffer.getElems();
            int[] source = (int[]) line;
            System.arraycopy(source, 0, dest, destArrayPos, elemCount);
        }
    }
}
