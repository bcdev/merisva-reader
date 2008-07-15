/*
 * $Id: HdfUtils.java,v 1.6 2005-08-04 22:55:51 marcoz Exp $
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
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;

import java.io.IOException;

import org.esa.beam.util.Debug;
import org.esa.beam.framework.dataio.ProductIOException;
import org.esa.beam.framework.datamodel.ProductData;


/**
 * A container class for hdf 5 accessor utility methods.
 */
class H5Utils {

    /**
     * Closes a hdf5 attribute.
     * @param id the attribute ID
     */
    public static void closeH5A(int id) {
        if (id >= 0) {
            try {
                H5.H5Aclose(id);
            } catch (HDF5LibraryException e) {
                Debug.trace(e);
            }
        }
    }

    /**
     * Opens a hdf5 group with given name at the givel location identifier
     * @param locID the location identifier
     * @param name the group name
     * @return the group identifier
     * @throws IOException
     */
    public static int openH5G(int locID, String name) throws IOException {
        int id = -1;

        try {
            id = H5.H5Gopen(locID, name);
        } catch (HDF5LibraryException e) {
            throw new ProductIOException(e.getMessage());
        }
        return id;
    }

    /**
     * Closes the group with given identifier
     * @param id the group identifier
     */
    public static void closeH5G(int id) {
        if (id >= 0) {
            try {
                H5.H5Gclose(id);
            } catch (HDF5LibraryException e) {
                Debug.trace(e);
            }
        }
    }

    /**
     * Reads a string attribute with given location ID and name
     * @param locId the location identifier
     * @param name the attribute name
     * @return the attribute value
     */
    public static String readStringAttribute(int locId, String name) {
        String strRet = null;
        int attributeId = -1;
        int typeId = -1;
        int max_length = -1;
        Object data = null;

        try {
            attributeId = H5.H5Aopen_name(locId, name);
            typeId = H5.H5Aget_type(attributeId);
            max_length = H5.H5Tget_size(typeId);
            data = new byte[max_length];
            H5.H5Aread(attributeId, typeId, data);
            strRet = new String((byte[]) data);
        } catch (HDF5Exception e) {
            Debug.trace("Unable to read attribute '" + name + "'");
        }
        finally {
            closeH5A(attributeId);
        }

        return strRet;
    }

    /**
     * Reads an integer attribute with given location ID and name
     * @param locId the location identifier
     * @param name the name
     * @return the attribute value
     */
    public static int readIntAttribute(int locId, String name) {
        int nRet = 0;
        int attributeId = -1;
        int typeId = -1;
        int[] data = new int[1];
        try {
            attributeId = H5.H5Aopen_name(locId, name);
            typeId = H5.H5Aget_type(attributeId);
            H5.H5Aread(attributeId, typeId, data);
            nRet = data[0];
        } catch (HDF5Exception e) {
            Debug.trace("Unable to read attribute '" + name + "'");
        } finally {
            closeH5A(attributeId);
        }

        return nRet;
    }

    /**
     * Reads a float attribute with given location ID and name
     * @param locId the location identifier
     * @param name the name
     * @return the attribute value
     */
    public static float readFloatAttribute(int locId, String name) {
        float fRet = 0.f;
        int attributeId = -1;
        int typeId = -1;
        float[] data = new float[1];

        try {
            attributeId = H5.H5Aopen_name(locId, name);
            typeId = H5.H5Aget_type(attributeId);
            H5.H5Aread(attributeId, typeId, data);
            fRet = data[0];
        } catch (HDF5Exception e) {
            Debug.trace("Unable to read attribute '" + name + "'");
        } finally {
            closeH5A(attributeId);
        }

        return fRet;
    }

    /**
     * Reads a double attribute with given location ID and name
     * @param locId the location identifier
     * @param name the name
     * @return the attribute value
     */
    public static double readDoubleAttribute(int locId, String name) {
        double dRet = 0.;
        int attributeId = -1;
        int typeId = -1;
        double[] data = new double[1];

        try {
            attributeId = H5.H5Aopen_name(locId, name);
            typeId = H5.H5Aget_type(attributeId);
            H5.H5Aread(attributeId, typeId, data);
            dRet = data[0];
        } catch (HDF5Exception e) {
            Debug.trace("Unable to read attribute '" + name + "'");
        } finally {
            closeH5A(attributeId);
        }

        return dRet;
    }

    /**
     * Converts a hdf5 datatype identifier to a beam product data type.
     * @param hdfTypeID the hdf5 datatype ID
     * @return the product data type identifier
     */
    public static int convertHdfToProductDataType(int hdfTypeID) {
        int nRet = ProductData.TYPE_UNDEFINED;
        int dataSign = -1;
        int dataClass = -1;
        int dataSize = -1;

        try {
            dataClass = H5.H5Tget_class(hdfTypeID);
            dataSize = H5.H5Tget_size(hdfTypeID);

            if (dataClass == HDF5Constants.H5T_FLOAT) {
                if (dataSize == 4) {
                    nRet = ProductData.TYPE_FLOAT32;
                } else if (dataSize == 8) {
                    nRet = ProductData.TYPE_FLOAT64;
                }
            } else if (dataClass == HDF5Constants.H5T_INTEGER) {
                dataSign = H5.H5Tget_sign(hdfTypeID);
                if (dataSign == HDF5Constants.H5T_SGN_NONE) {
                    if (dataSize == 1) {
                        nRet = ProductData.TYPE_UINT8;
                    } else if (dataSize == 2) {
                        nRet = ProductData.TYPE_UINT16;
                    } else if (dataSize == 4) {
                        nRet = ProductData.TYPE_UINT32;
                    }
                } else if (dataSign == HDF5Constants.H5T_SGN_2) {
                    if (dataSize == 1) {
                        nRet = ProductData.TYPE_INT8;
                    } else if (dataSize == 2) {
                        nRet = ProductData.TYPE_INT16;
                    } else if (dataSize == 4) {
                        nRet = ProductData.TYPE_INT32;
                    }
                }
            }
        } catch (HDF5LibraryException e) {
            Debug.trace(e);
        }

        return nRet;
    }
}
