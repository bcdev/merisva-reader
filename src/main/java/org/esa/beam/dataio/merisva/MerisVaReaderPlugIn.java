/*
 * $Id: MVAHdf5ReaderPlugIn.java,v 1.1.1.1 2003-06-30 16:50:24 tom Exp $
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

import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductReaderPlugIn;
import org.esa.beam.framework.dataio.DecodeQualification;
import org.esa.beam.util.io.BeamFileFilter;
import org.esa.beam.util.logging.BeamLogManager;

import java.io.File;
import java.util.Locale;

public class MerisVaReaderPlugIn implements ProductReaderPlugIn {

    public static final String MERIS_VA_FORMAT_NAME = "MERIS-VA";

    private static final String H5_FILE_EXTENSION = ".h5";
    private static final String MERIS_VA_DESCRIPTION = "MERIS-VA product reader";
    private static final String H5_CLASS_NAME = "ncsa.hdf.hdf5lib.H5";

    private static boolean h5LibraryAvailable = false;

    static {
        try {
            h5LibraryAvailable = Class.forName(H5_CLASS_NAME) != null;
        } catch (Throwable t) {
            // ignore, {@code h5LibraryAvailable} is already {@code false}
        }
        //noinspection StaticVariableUsedBeforeInitialization
        if (!h5LibraryAvailable) {
            BeamLogManager.getSystemLogger().info("HDF library is not available");
        }
    }

    /**
     * Returns whether or not the HDF5 library is available.
     */
    public static boolean isHdf5LibAvailable() {
        return h5LibraryAvailable;
    }

    public DecodeQualification getDecodeQualification(Object input) {
        if (!isHdf5LibAvailable()) {
            return DecodeQualification.UNABLE;
        }
        File file = null;

        if (input instanceof String) {
            file = new File((String) input);
        } else if (input instanceof File) {
            file = (File) input;
        }

        if (file != null && file.exists() && file.isFile()) {
            if (file.getPath().toLowerCase().endsWith(H5_FILE_EXTENSION)) {
                // @todo - check if the h5open testreading the file is really expensive
                // if not - implement this here
                return DecodeQualification.INTENDED;
            }
        }

        return DecodeQualification.UNABLE;
    }

    /**
     * Returns an array containing the classes that represent valid input types
     * for this reader.
     * <p/>
     * <p> Intances of the classes returned in this array are valid objects for the
     * <code>setInput</code> method of the <code>ProductReader</code> interface
     * (the method will not throw an <code>InvalidArgumentException</code> in this case).
     *
     * @return an array containing valid input types, never <code>null</code>
     */
    public Class[] getInputTypes() {
        if (!isHdf5LibAvailable()) {
            return new Class[0];
        }
        return new Class[]{String.class, File.class};
    }

    /**
     * Creates an instance of the actual product reader class.
     * This method should never return <code>null</code>.
     *
     * @return a new reader instance, never <code>null</code>
     */
    public ProductReader createReaderInstance() {
        if (!isHdf5LibAvailable()) {
            return null;
        }
        return new MerisVaReader(this);
    }


    /**
     * Creates an instance of the actual product writer class.
     * This method should never return <code>null</code>.
     *
     * @return a new writer instance, never <code>null</code>
     */
    public String[] getFormatNames() {
        if (!isHdf5LibAvailable()) {
            return new String[0];
        }
        return new String[]{MERIS_VA_FORMAT_NAME};
    }

    /**
     * Gets the default file extensions associated with each of the format names returned by the
     * <code>{@link #getFormatNames}</code> method.
     * <p>The string array returned shall always have the same lenhth as the array returned by the
     * <code>{@link #getFormatNames}</code> method.
     * <p>The extensions returned in the string array shall always include a leading colon ('.') character, e.g. <code>".hdf"</code>
     *
     * @return the default file extensions for this product I/O plug-in, never <code>null</code>
     */
    public String[] getDefaultFileExtensions() {
        if (!isHdf5LibAvailable()) {
            return new String[0];
        }
        return new String[]{H5_FILE_EXTENSION};
    }

    /**
     * Gets a short description of this plug-in.
     * If the given locale is set to <code>null</code> the default locale is used.
     * <p/>
     * <p> In a GUI, the description returned could be used as tool-tip text.
     *
     * @return a textual description of this product reader/writer
     */
    public String getDescription(Locale locale) {
        return MERIS_VA_DESCRIPTION;
    }

    public BeamFileFilter getProductFileFilter() {
        final String[] formatNames = getFormatNames();
        final String formatName;

        if (formatNames.length > 0) {
            formatName = formatNames[0];
        } else {
            formatName = "";
        }

        return new BeamFileFilter(formatName, getDefaultFileExtensions(), getDescription(null));
    }
}
