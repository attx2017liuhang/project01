public class FamilyAccount {
	public static void main(String[] args) {
        String details = "��֧\t�˻����\t��֧���\t˵    ��\n";
        int balance = 10000;

        System.out.println("hotfix");

        boolean loopFlag = true;
        do {
		    System.out.println("\n-----------------��ͥ��֧�������-----------------\n");
            System.out.println("                   1 ��֧��ϸ");
            System.out.println("                   2 �Ǽ�����");
            System.out.println("                   3 �Ǽ�֧��");
            System.out.println("                   4 ��    ��\n");
            System.out.print("                   ��ѡ��(1-4)��");
            
            char key = Utility.readMenuSelection();
            System.out.println();
            switch (key) {
                case '1':
                    System.out.println("-----------------��ǰ��֧��ϸ��¼-----------------");
                    System.out.println(details);
                    System.out.println("--------------------------------------------------");
                    break;
                case '2':
                    System.out.print("���������");
                    int amount1 = Utility.readNumber();
                    System.out.print("��������˵����");
                    String desc1 = Utility.readString();

                    balance += amount1;
                    details += "����\t" + balance + "\t\t" +
                               amount1 + "\t\t" + desc1 + "\n";
                    System.out.println("---------------------�Ǽ����---------------------");
                    break;
                case '3':
                    System.out.print("����֧����");
                    int amount2 = Utility.readNumber();
                    System.out.print("����֧��˵����");
                    String desc2 = Utility.readString();

                    balance -= amount2;
                    details += "֧��\t" + balance + "\t\t" +
                               amount2 + "\t\t" + desc2 + "\n";
                    System.out.println("---------------------�Ǽ����---------------------");
                    break;
                case '4':
                    System.out.print("ȷ���Ƿ��˳�(Y/N)��");
                    char yn = Utility.readConfirmSelection();
                    if (yn == 'Y') loopFlag = false;
                    break;
            }
        } while (loopFlag);
	}    
}
