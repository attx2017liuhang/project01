class FamilyAccount1 {
	public static void main(String[] args) {

		String details = "";//��¼��֧���
		int balance = 10000;//��ʼ���

		boolean isLoop = true;
		while (isLoop){

			System.out.println("\n--------------��ͥ��֧�������-----------\n");
			System.out.println("                   1 ��֧��ϸ");
			System.out.println("                   2 �Ǽ�����");
			System.out.println("                   3 �Ǽ�֧��");
			System.out.println("                   4 ��    ��\n");
			System.out.print("                     ��ѡ��1-4����");

			char menu = Utility.readMenuSelection();//��ȡ�û�������ַ���ѡ��1-4��
			switch (menu){
			case '1':
				System.out.println("--------------��ǰ��֧��ϸ��¼-----------");
				System.out.println("��֧\t�˻����\t��֧���\t˵  ��");
				System.out.println(details);
				System.out.println("-----------------------------------------");
				break;
			case '2':
				//System.out.println("                   2 �Ǽ�����");
				System.out.print("���������");
				int money = Utility.readNumber();//��ȡ�û������������
				System.out.print("��������˵����");
				String addMoney = Utility.readString();//��ȡ����˵�����ַ���
				balance += money;
				
				details += "����\t" + balance + "\t\t" + money + "\t\t" + addMoney + "\n";
				break;
			case '3':
				//System.out.println("                   3 �Ǽ�֧��");
				System.out.print("����֧����");
				int money1 = Utility.readNumber();//��ȡ�û������֧�����
				System.out.print("����֧��˵����");
				String minusMoney = Utility.readString();//��ȡ֧��˵�����ַ���
				
				if (balance >= money1){
					balance -= money1;
					details += "֧��\t" + balance + "\t\t" + money1 + "\t\t" + minusMoney + "\n";
				}else{
					System.out.println("֧�������˻���֧��ʧ�ܣ�");	
				}
				
				break;
			case '4':
				//System.out.println("                   4 ��    ��");
				//���û�ѡ��4������Yʱ���ڲ�ִ�У�isLoop = false
				System.out.println("\nȷ���Ƿ��˳���Y/N����");
				char isExit = Utility.readConfirmSelection();//��ȡUtility�����淽����ȡ�û����������
				if (isExit == 'Y'){
					isLoop = false;//��ִ���򲻻��ٴν���whileѭ��
				}
				break;//�����˳�switch-case�ṹ�������������ѭ��
		
			}
		}
	}
}
