package main.java.uk.ac.imperial.lsds.graphs;

import java.io.*;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Image;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;


public class xyLineChart extends ApplicationFrame {

	public xyLineChart(String applicationTitle, String chartTitle) {
		super(applicationTitle);
		JFreeChart xylineChart = ChartFactory.createXYLineChart(chartTitle,
				"Category", "Score", createDataset(), PlotOrientation.VERTICAL,
				true, true, false);

		ChartPanel chartPanel = new ChartPanel(xylineChart);
		chartPanel.setPreferredSize(new java.awt.Dimension(560, 367));
		final XYPlot plot = xylineChart.getXYPlot();
		XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
		renderer.setSeriesPaint(0, Color.RED);
		renderer.setSeriesPaint(1, Color.GREEN);
		renderer.setSeriesPaint(2, Color.YELLOW);
		renderer.setSeriesStroke(0, new BasicStroke(4.0f));
		renderer.setSeriesStroke(1, new BasicStroke(3.0f));
		renderer.setSeriesStroke(2, new BasicStroke(2.0f));
		plot.setRenderer(renderer);
		setContentPane(chartPanel);
	}
	
	private XYDataset parseMesosLoadStatsDataset(String filename){
		
		final XYSeriesCollection dataset = new XYSeriesCollection();
		final XYSeries slave1 = new XYSeries("wombat26");
		final XYSeries slave2 = new XYSeries("wombat27");
		final XYSeries slave3 = new XYSeries("wombat28");
		
		try (BufferedReader reader  = new BufferedReader(new FileReader(filename) )){ 
			
			//StringBuilder sb = new StringBuilder();
			double count = 1.0;
			String line =  reader.readLine();
			
			while( line != null ){
				//sb.append(line);
				//sb.append(System.lineSeparator());
				System.out.println("Read line " + line);
				int from = line.indexOf('[');
				int to = line.indexOf(']');
				String load1m = line.substring(from+1, to);
				
				ArrayList<String> stats = new ArrayList<String>(Arrays.asList(load1m.split(",")));
				
				System.out.println("##### Got stats "+ stats + " #####"); 
				
				slave1.add(count, Double.parseDouble(stats.get(0)));
				slave2.add(count, Double.parseDouble(stats.get(1)));
				slave3.add(count, Double.parseDouble(stats.get(2)));
				
				line = reader.readLine();
				count ++;
			}
			dataset.addSeries(slave1);
			dataset.addSeries(slave2);
			dataset.addSeries(slave3);
			
		} catch (FileNotFoundException e) {
			System.err.println("File "+ filename + " not found " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("File "+ filename + " io exception " + e.getMessage());
			e.printStackTrace();
		} 
	
		
		return dataset;
		
	}

	private XYDataset createDataset() {
		final XYSeries firefox = new XYSeries("Firefox");
		firefox.add(1.0, 1.0);
		firefox.add(2.0, 4.0);
		firefox.add(3.0, 3.0);
		final XYSeries chrome = new XYSeries("Chrome");
		chrome.add(1.0, 4.0);
		chrome.add(2.0, 5.0);
		chrome.add(3.0, 6.0);
		final XYSeries iexplorer = new XYSeries("InternetExplorer");
		iexplorer.add(3.0, 4.0);
		iexplorer.add(4.0, 5.0);
		iexplorer.add(5.0, 4.0);
		final XYSeriesCollection dataset = new XYSeriesCollection();
		dataset.addSeries(firefox);
		dataset.addSeries(chrome);
		dataset.addSeries(iexplorer);
		return dataset;
	}
	
	 /**
     * Creates PDf file.
     * @param outputStream {@link OutputStream}.
     * @throws DocumentException
     * @throws IOException
     */
    public static void create(JFreeChart chart,String tofile) throws DocumentException, IOException {
    	
    	
    	FileOutputStream outputStream = new FileOutputStream(new File(tofile));
    	
        Document document = null;
        PdfWriter writer = null;
         
        try {
            //instantiate document and writer
            document = new Document(PageSize.A4.rotate());
            writer = PdfWriter.getInstance(document, outputStream);
             
            //open document
            document.open();
             
            //add image
            int width = 800;
            int height = 500;
            
            BufferedImage bufferedImage = chart.createBufferedImage(width, height);
            /*
             * Quality tweak
             */
            Image image = Image.getInstance(writer, bufferedImage, 1.0f);
            
            /*
             * Alignment tweak
             */
            PdfPTable table = new PdfPTable(1);
            table.setWidthPercentage(100);
            // first movie
            table.getDefaultCell().setHorizontalAlignment(Element.ALIGN_CENTER);
            table.getDefaultCell().setVerticalAlignment(Element.ALIGN_TOP);
            
            PdfPCell cell = new PdfPCell(image);
            table.addCell(cell);
            document.add(table);
             
            //release resources
            document.close();
            document = null;
             
            writer.close();
            writer = null;
        } catch(DocumentException de) {
            throw de;
        } catch (IOException ioe) {
            throw ioe;
        } finally {
            //release resources
            if(null != document) {
                try { document.close(); }
                catch(Exception ex) { }
            }
             
            if(null != writer) {
                try { writer.close(); }
                catch(Exception ex) { }
            }
        }
    }

	public static void main(String[] args) throws IOException, DocumentException {

		
		xyLineChart chart = new xyLineChart("Browser Usage Statistics",
				"Which Browser are you using?");
		
		
		JFreeChart xylineChart = ChartFactory.createXYLineChart(
		         "Mesos Slave Nodes Load", 
		         "Time in seconds",
		         "Node Load", 
		         chart.parseMesosLoadStatsDataset("data/experiments/mesos_stats18-6-2015#08:05:56.log"),
		         PlotOrientation.VERTICAL, 
		         true, true, false);
		      
		      int width = 800; /* Width of the image */
		      int height = 600; /* Height of the image */ 
		      File XYChart = new File( "MesosLoadXYLineChart.jpeg" ); 
		      ChartUtilities.saveChartAsJPEG( XYChart, xylineChart, width, height);
		
		      create(xylineChart, "MesosLoadXYLineChart.pdf");
		
//		chart.pack();
//		RefineryUtilities.centerFrameOnScreen(chart);
//		chart.setVisible(true);
	}
}
