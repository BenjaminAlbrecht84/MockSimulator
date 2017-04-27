package util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class Downloader {

	public static File startFTP(FTPClient ftp, String remoteFolder, String remoteFile, File localDir) {

		try {
			
			if(ftp == null)
				return null;
				
			// change current directory
			ftp.changeWorkingDirectory(remoteFolder);

			FTPFile[] ftpFiles = ftp.listFiles();
			if (ftpFiles != null && ftpFiles.length > 0) {

				for (FTPFile file : ftpFiles) {

					if (file.getName().equals(remoteFile)) {

						System.out.println("Downloading file " + file.getName());

						// getting remote file
						OutputStream output = new FileOutputStream(localDir + "/" + file.getName());
						ftp.retrieveFile(file.getName(), output);
						output.close();

						return new File(localDir + "/" + file.getName());

					}

				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
		return null;
	}

}
