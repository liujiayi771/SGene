package edu.hust.elwg.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.broadinstitute.gatk.engine.recalibration.RecalDatum;
import org.broadinstitute.gatk.utils.QualityUtils;

/**
 * Created by shawn on 17-5-17.
 */

//简单的将表里的信息一行一行的解析，每一行为一个String，存在一个ArrayList里
public class MergeTables {
    public static void mergeTable(String[] oneFile, String[] twoFile, String oneOutputPath, String twoOutputPath){
        try{

            ArrayList<String []> grpfiles = new ArrayList<String []>();
            grpfiles.add(oneFile);
            grpfiles.add(twoFile);
            ArrayList<String> OutputPath = new ArrayList<String>();
            OutputPath.add(oneOutputPath);
            OutputPath.add(twoOutputPath);

            for(int counter = 0; counter < grpfiles.size(); counter++){
                ArrayList<ArrayList<String>> waitMergeTable = new ArrayList<>();
                ArrayList<String> finalTable;
                String[] tablePath = grpfiles.get(counter);
                for (int i = 0; i < tablePath.length; i++){
                    ArrayList<String> currentTable = new ArrayList<String>();
                    File file = new File(tablePath[i]);
                    if(file.isFile() && file.exists()){
                        InputStreamReader read = new InputStreamReader(new FileInputStream(file));
                        BufferedReader bufferedReader = new BufferedReader(read);
                        String line;
                        while((line = bufferedReader.readLine()) != null){
                            currentTable.add(line);
                        }
                        bufferedReader.close();
                        read.close();
                        waitMergeTable.add(currentTable);
//                    currentTable.clear();
                    }else{
                        System.out.println("Can't find the table");
                    }
                }

                //多个文件合并
                finalTable = waitMergeTable.get(0);
                for(int i = 1; i < waitMergeTable.size(); i++){
                    finalTable = mergeMethod(finalTable, waitMergeTable.get(i));
                }

                //修正finalTable的quality
                recalEmpQuality(finalTable);

//            double a = RecalDatum.bayesianEstimateOfEmpiricalQuality(495756, 195,34);
//            System.out.println("EmpQS == " + a);

//            printTable(finalTable);
                //写出合成的table

                File output = new File(OutputPath.get(counter));
                OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(output));
                BufferedWriter bufferedWriter = new BufferedWriter(writer);
                for(int i = 0; i < finalTable.size(); i++){
                    bufferedWriter.write(finalTable.get(i));
                    bufferedWriter.newLine();
                }
                bufferedWriter.close();
                writer.close();
            }


        } catch (Exception e){
            System.out.println("Merge Table failed");
            e.printStackTrace();
        }
    }

    //打印整张表的信息
    public static void printTable(ArrayList<String> table){
        if(table.size() != 0){
            for(int i = 0; i < table.size(); i++){
                System.out.println(table.get(i));
            }
        }else{
            System.out.println("The table is empty!");
        }
    }

    //修正该表的Empirical Quality,前提是保证该表里RGTable的EstimatedQ准确，
    public static void recalEmpQuality(ArrayList<String> finalTable){
        Map<String, Object> rgEventTypetoEmpQ = new HashMap<String, Object>();//QS节点通过eventType找到对应RG节点的EmpQuality
        Map<String, Object> qsEstiQtoEmpQ = new HashMap<String, Object>();//Cov节点通过EventType+EstimatedQ找到对应QS节点的EmpQuality
        int lineCounter = 120;
        ArrayList<String> RGRecalTable = getOneRecalTable(finalTable, lineCounter);
        for(int i = 3; i < RGRecalTable.size(); i++) {
            String[] str = RGRecalTable.get(i).split("\\s+");
            long Observations = Long.parseLong(str[4]);
            double Errors = Double.parseDouble(str[5]);
            double EstimatedQ = Double.parseDouble(str[3]);
            final long mismatches = (long)(Errors + 0.5) + 1L;
            final long observations = Observations + 2L;
//            System.out.println(mismatches + "---" + observations);
            //用贝叶斯估计法根据观测样本数，错误值，以及先验概率（EstimatedQReported）来计算后验概率（empiricalQual），建立了一个高斯模型和一个二项分布模型，用极大似然估计来得出最优的empiricalQual
            final double empiricalQual = RecalDatum.bayesianEstimateOfEmpiricalQuality(observations, mismatches, EstimatedQ);
            String finalLine = str[0] + " " + str[1] + " " + empiricalQual + " " + str[3] + " " + str[4] + " " + str[5];
            finalTable.set(i + lineCounter, finalLine);
            rgEventTypetoEmpQ.put(str[1], empiricalQual);
        }

        lineCounter = counterGrow(finalTable, lineCounter);
        ArrayList<String> QSRecalTable = getOneRecalTable(finalTable, lineCounter);

        for(int i = 3; i < QSRecalTable.size(); i++) {
            String[] str = QSRecalTable.get(i).split("\\s+");
            long Observations = Long.parseLong(str[4]);
            double Errors = Double.parseDouble(str[5]);
//            double EstimatedQ = Double.parseDouble(str[1]);
            final long mismatches = (long)(Errors + 0.5) + 1L;
            final long observations = Observations + 2L;
//            System.out.println(mismatches + "---" + observations);
            double empRG = (double)rgEventTypetoEmpQ.get(str[2]);
//            System.out.println("empRG == " + empRG);
            //用贝叶斯估计法根据观测样本数，错误值，以及先验概率（EstimatedQReported）来计算后验概率（empiricalQual），建立了一个高斯模型和一个二项分布模型，用极大似然估计来得出最优的empiricalQual
            final double empiricalQual = RecalDatum.bayesianEstimateOfEmpiricalQuality(observations, mismatches, empRG);
            String finalLine = str[0] + " " + str[1] + " " + str[2] + " " + empiricalQual + " " + str[4] + " " + str[5];
            finalTable.set(i + lineCounter, finalLine);
//            String a = str[2] + str[1];
//            System.out.println("key == " + a);
            qsEstiQtoEmpQ.put(str[2] + str[1], empiricalQual);//key is EventType + QualityScore
        }

        lineCounter = counterGrow(finalTable, lineCounter);
        ArrayList<String> CovRecalTable = getOneRecalTable(finalTable, lineCounter);

        for(int i = 3; i < CovRecalTable.size(); i++) {
            String[] str = CovRecalTable.get(i).split("\\s+");
            long Observations = Long.parseLong(str[6]);
            double Errors = Double.parseDouble(str[7]);
//            double EstimatedQ = Double.parseDouble(str[1]);
            final long mismatches = (long)(Errors + 0.5) + 1L;
            final long observations = Observations + 2L;
//            System.out.println(mismatches + "---" + observations);
            double empQS = (double)qsEstiQtoEmpQ.get(str[4] + str[1]);//key is EventType + QualityScore
//            System.out.println("empQS == " + empQS);
            //用贝叶斯估计法根据观测样本数，错误值，以及先验概率（EstimatedQReported）来计算后验概率（empiricalQual），建立了一个高斯模型和一个二项分布模型，用极大似然估计来得出最优的empiricalQual
            final double empiricalQual = RecalDatum.bayesianEstimateOfEmpiricalQuality(observations, mismatches, empQS);
            String finalLine = str[0] + " " + str[1] + " " + str[2] + " " + str[3] + " " + str[4] + " " + empiricalQual + " " + str[6] + " " + str[7];
            finalTable.set(i + lineCounter, finalLine);
        }
    }

    //将两个table除Empirical Quality以外的信息整合到一起
    public static ArrayList<String> mergeMethod(ArrayList<String> a, ArrayList<String> b){
        if(IsEmptyTable(a))
            return b;
        if(IsEmptyTable(b))
            return a;

        ArrayList<String> finalTable = new ArrayList<String>();
        //简单的将待整合的两表其中一表的Argument Table 和 Quantized Table赋给finalTable
        for(int counter = 0; counter < 120; counter++){
            finalTable.add(a.get(counter));
        }

        int lineCounterA = 120;
        int lineCounterB = 120;
        ArrayList<String> aRecalTable = getOneRecalTable(a, lineCounterA);
        ArrayList<String> bRecalTable = getOneRecalTable(b, lineCounterB);
//        System.out.println(lineCounterA);
//        System.out.println(lineCounterB);
//        printTable(aRecalTable);
//        printTable(bRecalTable);
//        ArrayList<String> cRecalTable = new ArrayList<String>();
        MergeRGTable(aRecalTable, bRecalTable, finalTable);
        lineCounterA = counterGrow(a, lineCounterA);
        lineCounterB = counterGrow(b, lineCounterB);
        aRecalTable = getOneRecalTable(a, lineCounterA);
        bRecalTable = getOneRecalTable(b, lineCounterB);
        finalTable.add("");
        MergeQSTable(aRecalTable, bRecalTable, finalTable);
        lineCounterA = counterGrow(a, lineCounterA);
        lineCounterB = counterGrow(b, lineCounterB);
        aRecalTable = getOneRecalTable(a, lineCounterA);
        bRecalTable = getOneRecalTable(b, lineCounterB);
//        printTable(aRecalTable);
//        printTable(bRecalTable);
        finalTable.add("");
        MergeCovTable(aRecalTable, bRecalTable, finalTable);
        finalTable.add("");
//        finalTable.add("");
        return finalTable;
    }

    //判断一个table表里的信息是不是空的
    public static boolean IsEmptyTable(ArrayList<String> a){
        String line = a.get(120);
        int row = getTableRow(line);
        if(row == 0)
            return true;
        else
            return false;
    }


    public static int getTableRow(String a){
        return Integer.parseInt(a.split(":")[3]);
    }

    public static int getTableCol(String a){
        return Integer.parseInt(a.split(":")[2]);
    }

    //除了简单相加Observation Error,还要计算EstimatedQ
    public static void MergeRGTable(ArrayList<String> a, ArrayList<String> b, ArrayList<String> c){
        for(int i = 0; i < 3; i++){
            c.add(a.get(i));
        }
        for(int i = 3; i < a.size(); i++) {
            long aObservations = Long.parseLong(a.get(i).split("\\s+")[4]);
            long bObservations = Long.parseLong(b.get(i).split("\\s+")[4]);
            long cObservations = aObservations + bObservations;
            double aErrors = Double.parseDouble(a.get(i).split("\\s+")[5]);
            double bErrors = Double.parseDouble(b.get(i).split("\\s+")[5]);
            double cErrors = aErrors + bErrors;
            double aEstimatedQ = Double.parseDouble(a.get(i).split("\\s+")[3]);
            double bEstimatedQ = Double.parseDouble(b.get(i).split("\\s+")[3]);
            double cEstimatedQ = calcEstimatedQReported(cObservations, aObservations, bObservations, aEstimatedQ, bEstimatedQ);
            String[] str = a.get(i).split("\\s+");
            String cLine = str[0] + " " + str[1] + " " + str[2] + " " + cEstimatedQ + " " + cObservations + " " + cErrors;
//            System.out.println("cLine == " +cLine);
            c.add(cLine);
        }
    }

    //合并两表后重新计算EstimatedQ，只有合并RGTable时用的到
    public static double calcEstimatedQReported(long cObservation, long aObservation, long bObservation, double aEstimatedQ, double bEstimatedQ){
        double sumErrors = aObservation * QualityUtils.qualToErrorProb(aEstimatedQ) + bObservation * QualityUtils.qualToErrorProb(bEstimatedQ);
        return -10 * Math.log10(sumErrors / cObservation);
    }

    public static void MergeQSTable(ArrayList<String> a, ArrayList<String> b, ArrayList<String> c){
        for(int i = 0; i < 3; i++){
            c.add(a.get(i));
        }
        long aObservations;
        long bObservations;
        long cObservations;
        double aErrors;
        double bErrors;
        double cErrors;
        int i = 3, j = 3;
        while(i < a.size() && j < b.size()){
            String[] aStr = a.get(i).split("\\s+");
            String[] bStr = b.get(j).split("\\s+");
//            for(int k = 0; k < aStr.length; k++){
//                System.out.println("aStr[i] == " + aStr[k]);
//                System.out.println("bStr[i] == " + bStr[k]);
//            }
//            System.out.println(a.get(i));
//            System.out.println(b.get(j));
            if(aStr[0].equals(bStr[0]) && aStr[1].equals(bStr[1]) && aStr[2].equals(bStr[2])){
                aObservations = Long.parseLong(aStr[4]);
                bObservations = Long.parseLong(bStr[4]);
                cObservations = aObservations + bObservations;
                aErrors = Double.parseDouble(aStr[5]);
                bErrors = Double.parseDouble(bStr[5]);
                cErrors = aErrors + bErrors;
                String cLine = aStr[0] + " " + aStr[1] + " " + aStr[2] + " " + aStr[3] + " " + cObservations + " " + cErrors;
                c.add(cLine);
                i++;
                j++;
            }else if(aStr[0].equals(bStr[0]) && aStr[2].equals(bStr[2])){
                if(Integer.parseInt(aStr[1]) < Integer.parseInt(bStr[1])){
                    c.add(a.get(i));
                    i++;
                }else{
                    c.add(b.get(j));
                    j++;
                }
            }else if(aStr[0].equals(bStr[0])){
                if(Integer.parseInt(aStr[2]) > Integer.parseInt(bStr[2])){//转换为int变量后，M > I > D
                    c.add(a.get(i));
                    i++;
                }else{
                    c.add(b.get(j));
                    j++;
                }
            }else{
                System.out.println("There are more than one readGroup, which can not be solved.");
            }
        }
        if(i < a.size()){
            for(;i < a.size();i++){
                c.add(a.get(i));
            }
        }else if(j < b.size()){
            for(;j < b.size();j++){
                c.add(b.get(j));
            }
        }
    }

    public static void MergeCovTable(ArrayList<String> a, ArrayList<String> b, ArrayList<String> c){
        for(int i = 0; i < 3; i++){
            c.add(a.get(i));
        }
        long aObservations;
        long bObservations;
        long cObservations;
        double aErrors;
        double bErrors;
        double cErrors;
        int i = 3, j = 3;
        while(i < a.size() && j < b.size()){
            String[] aStr = a.get(i).split("\\s+");
            String[] bStr = b.get(j).split("\\s+");
            int signal = covariateLineCompare(aStr[1], aStr[2], aStr[3], aStr[4], bStr[1], bStr[2], bStr[3], bStr[4]);
//            System.out.println(signal + " " + aStr[1] + " " + aStr[2] + " " + aStr[3] + " " + aStr[4] + " " + bStr[1] + " " + bStr[2] + " " + bStr[3] + " " + bStr[4] );
            if(signal == 0){
                aObservations = Long.parseLong(aStr[6]);
                bObservations = Long.parseLong(bStr[6]);
                cObservations = aObservations + bObservations;
                aErrors = Double.parseDouble(aStr[7]);
                bErrors = Double.parseDouble(bStr[7]);
                cErrors = aErrors + bErrors;
                String cLine = aStr[0] + " " + aStr[1] + " " + aStr[2] + " " + aStr[3] + " " + aStr[4] + " " + aStr[5] + " " + cObservations + " " + cErrors;
                c.add(cLine);
                i++;
                j++;
            }else if(signal < 0){
                c.add(a.get(i));
                i++;
            }else{
                c.add(b.get(j));
                j++;
            }
        }
        if(i < a.size()){
            for(;i < a.size();i++){
                c.add(a.get(i));
            }
        }else if(j < b.size()){
            for(;j < b.size();j++){
                c.add(b.get(j));
            }
        }
    }

    //从table表里取出一个RecalTable
    public static ArrayList<String> getOneRecalTable(ArrayList<String> a, int lineCounter){
        ArrayList<String> oneRecalTable = new ArrayList<String>();
        String tableSize = a.get(lineCounter);
        int lastLine = lineCounter + 3 + getTableRow(tableSize);
        for(int i = lineCounter; i < lastLine; i++){
            oneRecalTable.add(a.get(i));
        }
//        lineCounter = lastLine;
        return oneRecalTable;
    }

    //使lineCounter指向下一个RecalTable的size信息行　(example:  #:GATKTable:8:1240:%s:%s:%s:%s:%s:%.4f:%d:%.2f:;)
    public static int counterGrow(ArrayList<String> a, int lineCounter){
        return lineCounter + 3 + getTableRow(a.get(lineCounter)) + 1;
    }

    //if a < b, return －1; a == b, return 0; a > b, return 1.　return -1, add a; return 1 add b.
    public static int covariateLineCompare(String aQualityScore,String aCovariateValue, String aCovariateName, String aEventType, String bQualityScore,String bCovariateValue, String bCovariateName, String bEventType) {
        if (aCovariateName.equals(bCovariateName) && aCovariateName.equals("Context"))
            if (aQualityScore.equals(bQualityScore))
                if (contextValueCompare(aCovariateValue, bCovariateValue) == 0)
                    if (eventTypeCompare(aEventType, bEventType) == 0)
                        return 0;
                    else
                        return eventTypeCompare(aEventType, bEventType);// return (b-a)  a、b {M、 D、 I}
                else
                    return contextValueCompare(aCovariateValue, bCovariateValue);
            else
                return aQualityScore.compareTo(bQualityScore);
        else if (aCovariateName.equals(bCovariateName) && aCovariateName.equals("Cycle"))
            if (aQualityScore.equals(bQualityScore))
                if (cycleValueCompare(aCovariateValue, bCovariateValue) == 0)
                    return eventTypeCompare(aEventType, bEventType);
                else
                    return cycleValueCompare(aCovariateValue, bCovariateValue);
            else
                return aQualityScore.compareTo(bQualityScore);

        else if (aCovariateName.equals("Context"))
            return -1;
        else
            return 1;
    }

    //if a == b, return 0; a < b,return -1; a > b return 1. 　A < C < G < T
    public static int contextValueCompare(String aCovariateValue, String bCovariateValue){
        String a;
        String b;
        if(aCovariateValue.length() == bCovariateValue.length()){
            a = aCovariateValue;
            b = bCovariateValue;
        } else if(aCovariateValue.length() < bCovariateValue.length()) {
            a = aCovariateValue + 'A';
            b = bCovariateValue;
        } else {
            a = aCovariateValue;
            b = bCovariateValue + 'A';
        }
        return reverse(a).compareTo(reverse(b));
    }

    //反转一个字符串
    public static String reverse(String s) {
        int length = s.length();
        String reverse = "";
        for (int i = 0; i < length; i++)
            reverse = s.charAt(i) + reverse;
        return reverse;
    }

    //M > I > D
    public static int eventTypeCompare(String aEventType, String bEventType){
        return bEventType.compareTo(aEventType);
    }

    public static int cycleValueCompare(String aCycle, String bCycle){
        if(aCycle.equals(bCycle))
            return 0;
        int a = Integer.parseInt(aCycle);
        int b = Integer.parseInt(bCycle);
        if(a + b == 0)
            if(a < 0)
                return 1;
            else
                return -1;
        if(Math.abs(a) < Math.abs(b))
            return -1;
        else
            return 1;
    }
}
