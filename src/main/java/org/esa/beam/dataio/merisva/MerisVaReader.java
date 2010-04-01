/*
 * $Id: MVAHdf5Reader.java,v 1.15 2006-01-11 14:40:24 tom Exp $
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

import com.bc.ceres.core.ProgressMonitor;
import ncsa.hdf.hdf5lib.H5;
import ncsa.hdf.hdf5lib.HDF5Constants;
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception;
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException;
import org.esa.beam.framework.dataio.AbstractProductReader;
import org.esa.beam.framework.dataio.IllegalFileFormatException;
import org.esa.beam.framework.dataio.ProductIOException;
import org.esa.beam.framework.dataio.ProductReaderPlugIn;
import org.esa.beam.framework.datamodel.*;
import org.esa.beam.util.Debug;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

class MerisVaReader extends AbstractProductReader {

    private static boolean _h5Initialized = false;
    private int _fileId;
    private Product _product;
    private String _productName;
    private String _productType;
    private int _width;
    private int _height;
    private int _tiePtColCount;
    private int _tiePtLineCount;
    private int _tiePtSubs;
    private Hashtable _bands;
    private int _sceneGrpID;
    private TiePointGrid _latGrid;
    private TiePointGrid _lonGrid;
    private Vector _flagsDsNames;


    /**
     * Construct a new instance of a product reader for the Meris-VA Hdf5
     * product reader plug-in.
     *
     * @param readerPlugIn the given Meris-VA Hdf5 product writer plug-in, must not be <code>null</code>
     */
    public MerisVaReader(ProductReaderPlugIn readerPlugIn) {
        super(readerPlugIn);
        _fileId = -1;
        _product = null;
        _flagsDsNames = new Vector();
    }

    /**
     * Provides an implementation of the <code>readProductNodes</code> interface method.
     * Clients implementing this method can be sure that the input object and eventually
     * the subset information has already been set.
     * <p/>
     * <p>This method is called as a last step in the <code>readProductNodes(input, subsetInfo)</code>
     * method.
     *
     * @throws java.io.IOException if an I/O error occurs
     * @throws org.esa.beam.framework.dataio.IllegalFileFormatException
     *                             if the input file in not decodeable
     */
    protected Product readProductNodesImpl() throws IOException, IllegalFileFormatException {
        assureHdfLibInitialized();
        File inputFile;
        if (getInput() instanceof String) {
            inputFile = new File((String) getInput());
        } else if (getInput() instanceof File) {
            inputFile = (File) getInput();
        } else {
            throw new IllegalArgumentException("unsupported input source: " + getInput());
        }

        try {
            _fileId = H5.H5Fopen(inputFile.getPath(), HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
        } catch (HDF5LibraryException e) {
            throw new ProductIOException(createErrorMessage(e));
        }

        readMandatoryAttributes();
        createProduct();

        // add all metadata if required
        // ----------------------------
        if (!isMetadataIgnored()) {
            readAttributes();
            readTiePointGrids();
            setUpGeoCoding();
        }
        readBands();

        return _product;
    }

    /**
     * The template method which is called by the {@link org.esa.beam.framework.dataio.AbstractProductReader#readBandRasterDataImpl} method after
     * an optional spatial subset has been applied to the input parameters.
     * <p/>
     * <p>The destination band, buffer and region parameters are exactly the
     * ones passed to the original {@link org.esa.beam.framework.dataio.AbstractProductReader#readBandRasterDataImpl} call. Since the
     * <code>destOffsetX</code> and <code>destOffsetY</code> parameters are
     * already taken into acount in the <code>sourceOffsetX</code> and
     * <code>sourceOffsetY</code> parameters, an implementor of this method is free to
     * ignore them.
     *
     * @param sourceOffsetX the absolute X-offset in source raster co-ordinates
     * @param sourceOffsetY the absolute Y-offset in source raster co-ordinates
     * @param sourceWidth   the width of region providing samples to be read given in source raster co-ordinates
     * @param sourceHeight  the height of region providing samples to be read given in source raster co-ordinates
     * @param sourceStepX   the sub-sampling in X direction within the region providing samples to be read
     * @param sourceStepY   the sub-sampling in Y direction within the region providing samples to be read
     * @param destBand      the destination band which identifies the data source from which to read the sample values
     * @param destBuffer    the destination buffer which receives the sample values to be read
     * @param destOffsetX   the X-offset in the band's raster co-ordinates
     * @param destOffsetY   the Y-offset in the band's raster co-ordinates
     * @param destWidth     the width of region to be read given in the band's raster co-ordinates
     * @param destHeight    the height of region to be read given in the band's raster co-ordinates
     *
     * @throws java.io.IOException if  an I/O error occurs
     * @see #getSubsetDef
     */
    @Override
    protected void readBandRasterDataImpl(int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight,
                                          int sourceStepX, int sourceStepY, Band destBand, int destOffsetX,
                                          int destOffsetY,
                                          int destWidth, int destHeight, ProductData destBuffer,
                                          ProgressMonitor pm) throws IOException {
        assureHdfLibInitialized();

        final MerisVaBand band = (MerisVaBand) _bands.get(destBand.getName());
        final int sourceMaxX = sourceOffsetX + sourceWidth - 1;
        final int sourceMaxY = sourceOffsetY + sourceHeight - 1;
        int destArrayPos = 0;

        if (band != null) {
            try {
                pm.beginTask("Reading band '" + destBand.getName() + "'...", destHeight); /*I18N*/

                for (int sourceY = sourceOffsetY; sourceY <= sourceMaxY; sourceY += sourceStepY) {
                    if (pm.isCanceled()) {
                        break;
                    }
                    band.readRasterLine(sourceOffsetX, sourceMaxX, sourceStepX, sourceY, destBuffer, destArrayPos);
                    destArrayPos += destWidth;
                    pm.worked(1);
                }
            } finally {
                pm.done();
            }
        }
    }

    /**
     * Closes the hdf5 MVA reader.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        MerisVaBand band;
        Enumeration elements = _bands.elements();

        while (elements.hasMoreElements()) {
            band = (MerisVaBand) elements.nextElement();
            band.close();
        }

        if (_sceneGrpID >= 0) {
            H5Utils.closeH5G(_sceneGrpID);
        }

        try {
            H5.H5Fclose(_fileId);
        } catch (HDF5LibraryException e) {
            throw new ProductIOException(createErrorMessage(e));
        }
        _fileId = -1;
    }

    ///////////////////////////////////////////////////////////////////////////
    /////// END OF PUBLIC
    ///////////////////////////////////////////////////////////////////////////

    /**
     * Checks for the initialization state of the hdf5 library. If it is not initialized
     * initializes the library
     *
     * @throws ProductIOException on initialization failure
     */
    private void assureHdfLibInitialized() throws ProductIOException {
        if (!_h5Initialized) {
            try {
                H5.H5open();
                _h5Initialized = true;
            } catch (HDF5LibraryException e) {
                throw new ProductIOException(createErrorMessage(e));
            }
        }
    }

    /**
     * Creates a special error string for hdf library errors
     *
     * @param e the HDF5Exception caught
     *
     * @return the message
     */
    private String createErrorMessage(HDF5Exception e) {
        return "HDF library error: " + e.getMessage();
    }

    /**
     * Creates the <code>Product</code> from the metadata given in the h5 file.
     */
    private void createProduct() throws IOException {
        int rootGrpID = -1;

        try {
            rootGrpID = H5Utils.openH5G(_fileId, MerisVaConstants.ROOT_GROUP_NAME);
            _productName = H5Utils.readStringAttribute(rootGrpID, MerisVaConstants.PRODUCT_NAME_ATT_NAME);
            _productType = H5Utils.readStringAttribute(rootGrpID, MerisVaConstants.PRODUCT_TYPE_ATT_NAME);

            _product = new Product(_productName, _productType, _width, _height);
            _product.setProductReader(this);
        } catch (IOException e) {
            throw e;
        } finally {
            if (rootGrpID >= 0) {
                H5Utils.closeH5G(rootGrpID);
            }
        }
    }

    /**
     * Reads the most important attributes for the product. These are the scenen width and hight,
     * tie point swidth, height and subsampling. Throws exception if one of those is missing.
     */
    private void readMandatoryAttributes() throws IOException {
        int rootGrpID = -1;

        try {
            rootGrpID = H5Utils.openH5G(_fileId, MerisVaConstants.ROOT_GROUP_NAME);

            _width = H5Utils.readIntAttribute(rootGrpID, MerisVaConstants.SCENE_WIDTH_ATT_NAME);
            _height = H5Utils.readIntAttribute(rootGrpID, MerisVaConstants.SCENE_HEIGHT_ATT_NAME);
            _tiePtColCount = H5Utils.readIntAttribute(rootGrpID, MerisVaConstants.TIE_PT_COL_CNT_ATT_NAME);
            _tiePtLineCount = H5Utils.readIntAttribute(rootGrpID, MerisVaConstants.TIE_PT_LINE_CNT_ATT_NAME);
            _tiePtSubs = H5Utils.readIntAttribute(rootGrpID, MerisVaConstants.TIE_PT_SUBS_ATT_NAME);

        } catch (IOException e) {
            throw e;
        } finally {
            if (rootGrpID >= 0) {
                H5Utils.closeH5G(rootGrpID);
            }
        }
    }

    /**
     * Reads all the high-level attributes from the file
     *
     * @throws IOException
     */
    private void readAttributes() throws IOException {
        int rootGrpID = -1;
        int numAttrs = 0;
        MetadataElement mdElem = null;
        MetadataElement mphElem = null;

        try {
            mdElem = _product.getMetadataRoot();
            if (mdElem == null) {
                return;
            }
            mphElem = new MetadataElement("MPH");
            rootGrpID = H5Utils.openH5G(_fileId, MerisVaConstants.ROOT_GROUP_NAME);
            numAttrs = H5.H5Aget_num_attrs(rootGrpID);

            // loop over attributes. Catch exceptions here, we don't want to lose
            // all attributes because of one failure
            for (int n = 0; n < numAttrs; n++) {
                try {
                    addAttributeToMetadata(rootGrpID, n, mphElem);
                } catch (IOException e) {
                    Debug.trace("Unable to read attribute at index '" + n + "'");
                    Debug.trace(e);
                } finally {
                }
            }

            mdElem.addElement(mphElem);
        } catch (HDF5LibraryException e) {
            throw new IOException(e.getMessage());
        } catch (IOException e) {
            throw e;
        } finally {
            if (rootGrpID >= 0) {
                H5Utils.closeH5G(rootGrpID);
            }
        }
    }

    /**
     * Adds an attribute to the metadata element passed in.
     *
     * @param groupID the group identifier
     * @param idx     the attribute index inside the group
     * @param target  the target MetadataElement
     *
     * @throws IOException
     */
    private void addAttributeToMetadata(int groupID, int idx, MetadataElement target) throws IOException {
        int typeClass;
        int attrID = -1;
        int typeID = -1;
        String name[] = {""};
        MetadataAttribute attribute = null;
        ProductData prodData = null;

        try {
            attrID = H5.H5Aopen_idx(groupID, idx);
            H5.H5Aget_name(attrID, 80, name);

            typeID = H5.H5Aget_type(attrID);
            typeClass = H5.H5Tget_class(typeID);
        } catch (HDF5LibraryException e) {
            throw new IOException(e.getMessage());
        } finally {
            if (attrID >= 0) {
                try {
                    H5.H5Aclose(attrID);
                } catch (HDF5LibraryException e) {
                    Debug.trace(e);
                }
            }
        }

        if (typeClass == HDF5Constants.H5T_INTEGER) {
            int value = H5Utils.readIntAttribute(groupID, name[0]);
            prodData = ProductData.createInstance(new int[]{value});
        } else if (typeClass == HDF5Constants.H5T_FLOAT) {
            float value = H5Utils.readFloatAttribute(groupID, name[0]);
            prodData = ProductData.createInstance(new float[]{value});
        } else if (typeClass == HDF5Constants.H5T_STRING) {
            String value = H5Utils.readStringAttribute(groupID, name[0]);
            prodData = ProductData.createInstance(value);
        } else {
            return;
        }

        attribute = new MetadataAttribute(name[0], prodData, true);
        target.addAttribute(attribute);
    }

    /**
     * Reads the bands of the "SceneData" group and adds them to the product.
     */
    private void readBands() throws IOException {
        int nDatasets = 0;
        int n;
        String[] names = new String[1];
        int[] types = new int[1];

        try {
            _bands = new Hashtable();
            _sceneGrpID = H5Utils.openH5G(_fileId, MerisVaConstants.SCENE_GROUP_NAME);

            nDatasets = H5.H5Gn_members(_fileId, MerisVaConstants.SCENE_GROUP_NAME);

            for (n = 0; n < nDatasets; n++) {
                H5.H5Gget_obj_info_idx(_fileId, MerisVaConstants.SCENE_GROUP_NAME, n, names, types);
                addBandToProduct(_sceneGrpID, names[0]);
            }

            setFlagCodingForFlagBands();
        } catch (HDF5LibraryException e) {
            throw new ProductIOException(createErrorMessage(e));
        } catch (IOException e) {
            throw e;
        } finally {
        }
    }

    /**
     * Adds a band to the product with given group identifier and object name.
     *
     * @param grpID group identifier
     * @param name  object name
     */
    private void addBandToProduct(int grpID, String name) throws IOException {
        int datasetID = -1;
        int dataspaceID = -1;
        int dataTypeID = -1;
        int nRank = 0;
        int dataType = ProductData.TYPE_UNDEFINED;
        long[] dims = new long[2];
        long[] dimsMax = new long[2];
        Band band;
        MerisVaBand hdfBand;

        try {
            datasetID = H5.H5Dopen(grpID, name);
            dataspaceID = H5.H5Dget_space(datasetID);

            nRank = H5.H5Sget_simple_extent_ndims(dataspaceID);
            if (nRank != 2) {
                throw new ProductIOException("Invalid dataset rank: " + name + " rank = " + nRank);
            }

            H5.H5Sget_simple_extent_dims(dataspaceID, dims, dimsMax);
            if ((dims[0] != _height) || (dims[1] != _width)) {
                throw new ProductIOException(
                        "Invalid dataset size: " + name + " width = " + dims[1] + " height = " + dims[0]);
            }

            dataTypeID = H5.H5Dget_type(datasetID);
            dataType = H5Utils.convertHdfToProductDataType(dataTypeID);
            if (dataType == ProductData.TYPE_UNDEFINED) {
                throw new ProductIOException("Invalid dataset data type: " + name);
            }
            band = new Band(name, dataType, _width, _height);

            // read attributes
            // ---------------
            String description = H5Utils.readStringAttribute(datasetID, MerisVaConstants.DESCRIPTION_ATT_NAME);
            if (description != null) {
                band.setDescription(description);
            }
            String unit = H5Utils.readStringAttribute(datasetID, MerisVaConstants.UNIT_ATT_NAME);
            if (unit != null) {
                band.setUnit(unit);
            }

            String flagsDs = H5Utils.readStringAttribute(datasetID, MerisVaConstants.FLAGS_DS_ATT_NAME);
            if (flagsDs != null) {
                flagsDs = flagsDs.trim();
                if (flagsDs.length() > 0) {
                    addNameToFlagsDs(flagsDs);
                }
            }

            double scalingFactor = H5Utils.readDoubleAttribute(datasetID, MerisVaConstants.SCALE_ATT_NAME);
            if (scalingFactor != 0.0) {
                band.setScalingFactor(scalingFactor);
            }
            double scalingOffset = H5Utils.readDoubleAttribute(datasetID, MerisVaConstants.OFFSET_ATT_NAME);
            if (scalingOffset != 0.0) {
                band.setScalingOffset(scalingOffset);
            }

            _product.addBand(band);

            hdfBand = new MerisVaBand();
            hdfBand.init(datasetID, dataspaceID, dataTypeID);
            _bands.put(name, hdfBand);
        } catch (HDF5LibraryException e) {
            throw new ProductIOException(createErrorMessage(e));
        } finally {
        }
    }

    /**
     * Reads all bands of the tie point group and adds them to the product.
     *
     * @throws IOException
     */
    private void readTiePointGrids() throws IOException {
        int tiePtGrpID = -1;
        int nDatasets = 0;
        int n;
        String[] names = new String[1];
        int[] types = new int[1];

        try {
            tiePtGrpID = H5Utils.openH5G(_fileId, MerisVaConstants.TIE_POINT_GROUP_NAME);
            nDatasets = H5.H5Gn_members(_fileId, MerisVaConstants.TIE_POINT_GROUP_NAME);

            for (n = 0; n < nDatasets; n++) {
                H5.H5Gget_obj_info_idx(_fileId, MerisVaConstants.TIE_POINT_GROUP_NAME, n, names, types);
                addTiePointGridToProduct(tiePtGrpID, names[0]);
            }
        } catch (HDF5LibraryException e) {
            throw new ProductIOException(createErrorMessage(e));
        } catch (IOException e) {
            throw e;
        } finally {
            if (tiePtGrpID >= 0) {
                H5Utils.closeH5G(tiePtGrpID);
            }
        }
    }

    /**
     * Adds the tie point grid with the given name and group to the product.
     *
     * @param grpID the group identifier where the tie point grid resides
     * @param name  the name of the tie point grid
     *
     * @throws IOException
     */
    private void addTiePointGridToProduct(int grpID, String name) throws IOException {
        int datasetID = -1;
        int dataspaceID = -1;
        int hdfDataType = -1;
        int prodDataType = ProductData.TYPE_UNDEFINED;
        int nRank = 0;
        int width;
        int height;
        long[] dims = new long[2];
        long[] dimsMax = new long[2];
        float[] data = null;
        TiePointGrid grid;
        String description = null;
        String unit = null;


        try {
            datasetID = H5.H5Dopen(grpID, name);
            dataspaceID = H5.H5Dget_space(datasetID);

            // check rank
            nRank = H5.H5Sget_simple_extent_ndims(dataspaceID);
            if (nRank != 2) {
                throw new ProductIOException("Invalid tie point grid rank: " + name + " rank = " + nRank);
            }

            // chack data type
            hdfDataType = H5.H5Dget_type(datasetID);
            prodDataType = H5Utils.convertHdfToProductDataType(hdfDataType);
            if (prodDataType != ProductData.TYPE_FLOAT32) {
                throw new ProductIOException("Invalid tie point grid data type: " + name);
            }

            // check dimensions and read data
            H5.H5Sget_simple_extent_dims(dataspaceID, dims, dimsMax);
            width = (int) dims[1];
            height = (int) dims[0];
            if ((width != _tiePtColCount) || (height != _tiePtLineCount)) {
                throw new ProductIOException(
                        "Invalid tie point grid size: " + name + " width = " + dims[1] + " height = " + dims[0]);
            }
            data = new float[width * height];
            H5.H5Dread(datasetID, hdfDataType, HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                       HDF5Constants.H5P_DEFAULT, data);
            grid = new TiePointGrid(name, width, height, 0.5f, 0.5f, _tiePtSubs, _tiePtSubs, data);

            // read attributes
            description = H5Utils.readStringAttribute(datasetID, MerisVaConstants.DESCRIPTION_ATT_NAME);
            if (description != null) {
                grid.setDescription(description);
            }
            unit = H5Utils.readStringAttribute(datasetID, MerisVaConstants.UNIT_ATT_NAME);
            if (unit != null) {
                grid.setUnit(unit);
            }

            _product.addTiePointGrid(grid);

            // check for geocoding
            // -------------------
            if (name.equalsIgnoreCase(MerisVaConstants.LAT_TIE_POINT_NAME)) {
                _latGrid = grid;
            }
            if (name.equalsIgnoreCase(MerisVaConstants.LON_TIE_POINT_NAME)) {
                _lonGrid = grid;
            }
        } catch (HDF5Exception e) {
            if (e instanceof HDF5LibraryException) {
                HDF5LibraryException libException = (HDF5LibraryException) e;
                throw new ProductIOException(
                        libException.getMessage() + " " + libException.getMajorErrorNumber() + " " + libException.getMinorErrorNumber());
            } else {
                throw new ProductIOException(e.getMessage());
            }
        } finally {
            if (dataspaceID >= 0) {
                try {
                    H5.H5Sclose(dataspaceID);
                } catch (HDF5LibraryException e) {
                    Debug.trace(e);
                }
            }

            if (datasetID >= 0) {
                try {
                    H5.H5Dclose(datasetID);
                } catch (HDF5LibraryException e) {
                    Debug.trace(e);
                }
            }
        }
    }

    /**
     * Adds the geocoding information to the product.
     */
    private void setUpGeoCoding() {
        if ((_latGrid != null) && (_lonGrid != null)) {
            GeoCoding coding = new TiePointGeoCoding(_latGrid, _lonGrid);
            _product.setGeoCoding(coding);
        }
    }

    /**
     * Adds a band nam to9 the list of detected flags dataset names.
     * Checks if the name is already in the list and if so skips the insertion.
     *
     * @param name the band name to be added
     */
    private void addNameToFlagsDs(String name) {
        String listName = null;
        boolean bSkip = false;

        for (int n = 0; n < _flagsDsNames.size(); n++) {
            listName = (String) _flagsDsNames.elementAt(n);
            if (listName.equalsIgnoreCase(name)) {
                bSkip = true;
                break;
            }
        }

        if (!bSkip) {
            _flagsDsNames.add(name);
        }
    }

    /**
     * Adds flag a coding to all flag bands.
     */
    private void setFlagCodingForFlagBands() throws IOException {
        Band productBand = null;
        MerisVaBand mvaBand = null;
        String currentName;
        FlagCoding flagCoding = null;

        for (int n = 0; n < _flagsDsNames.size(); n++) {
            currentName = (String) _flagsDsNames.elementAt(n);
            productBand = _product.getBand(currentName);
            mvaBand = (MerisVaBand) _bands.get(currentName);

            if ((productBand != null) && (mvaBand != null)) {
                flagCoding = createFlagCoding(mvaBand, currentName);
                productBand.setFlagCoding(flagCoding);
                _product.addFlagCoding(flagCoding);

                addDefaultBitmaskDefsToProduct(flagCoding);
            }
        }
    }

    /**
     * Tries to create a flag coding from the hdf5 bands attributes.
     *
     * @param band the band from which the attributes shall be extracted.
     *
     * @return a flag coding - or null on failures
     */
    private FlagCoding createFlagCoding(MerisVaBand band, String flagBandName) throws IOException {
        FlagCoding coding;
        String attribValue = new String();
        String attribName;
        //String codingName;
        int datasetId = band.getDatasetID();
        int idx = 0;
        String[] flagNames = new String[]{""};
        String[] flagDescriptions = new String[]{""};

        //codingName = HdfUtils.readStringAttribute(datasetId, MVAConstants.DESCRIPTION_ATT_NAME);
        //codingName = StringUtils.createValidName(codingName, null, '_');
        coding = new FlagCoding(flagBandName);

        while (attribValue != null) {
            attribName = MerisVaConstants.FLAG_CODE_PATTERN + (idx + 1);
            attribValue = H5Utils.readStringAttribute(datasetId, attribName);

            if (attribValue != null) {
                splitAttributeString(flagNames, flagDescriptions, attribValue);

                coding.addFlag(flagNames[0], 1 << idx, flagDescriptions[0]);
            }
            ++idx;
        }
        return coding;
    }

    private void splitAttributeString(String[] flagNames, String[] flagDescriptions, String attValue) {
        int firstPos = -1;
        int sepStringLength = 0;
        flagNames[0] = null;
        flagDescriptions[0] = null;

        for (int n = 0; n < MerisVaConstants.FLAG_SEPARATION_STRINGS.length; n++) {
            firstPos = attValue.indexOf(MerisVaConstants.FLAG_SEPARATION_STRINGS[n]);
            if (firstPos > 0) {
                sepStringLength = MerisVaConstants.FLAG_SEPARATION_STRINGS[n].length();
                flagNames[0] = attValue.substring(0, firstPos);
                flagDescriptions[0] = attValue.substring(firstPos + sepStringLength, attValue.length());
                break;
            }
        }
    }

    private void addDefaultBitmaskDefsToProduct(FlagCoding coding) {
        String[] flagNames = coding.getFlagNames();
        String expression;

        Color[] colors = new Color[]{Color.YELLOW, Color.CYAN, Color.ORANGE, Color.MAGENTA, Color.GREEN, Color.PINK, Color.RED, Color.BLUE};
        int colorIdx = 0;

        for (int n = 0; n < flagNames.length; n++) {
            expression = coding.getName() + "." + flagNames[n];
            _product.addBitmaskDef(
                    new BitmaskDef(flagNames[n].toLowerCase(), null, expression, colors[colorIdx], 0.5f));
            ++colorIdx;
            if (colorIdx >= colors.length) {
                colorIdx = 0;
            }
        }
    }
}
