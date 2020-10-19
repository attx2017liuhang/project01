class FamilyAccount1 {
	public static void main(String[] args) {

		String details = "";//记录收支情况
		int balance = 10000;//初始金额

		boolean isLoop = true;
		while (isLoop){

			System.out.println("\n--------------家庭收支记账软件-----------\n");
			System.out.println("                   1 收支明细");
			System.out.println("                   2 登记收入");
			System.out.println("                   3 登记支出");
			System.out.println("                   4 退    出\n");
			System.out.print("                     请选择（1-4）；");

			char menu = Utility.readMenuSelection();//读取用户输入的字符型选择（1-4）
			switch (menu){
			case '1':
				System.out.println("--------------当前收支明细记录-----------");
				System.out.println("收支\t账户金额\t收支金额\t说  明");
				System.out.println(details);
				System.out.println("-----------------------------------------");
				break;
			case '2':
				//System.out.println("                   2 登记收入");
				System.out.print("本次收入金额：");
				int money = Utility.readNumber();//获取用户输入的收入金额
				System.out.print("本次收入说明：");
				String addMoney = Utility.readString();//获取收入说明的字符串
				balance += money;
				
				details += "收入\t" + balance + "\t\t" + money + "\t\t" + addMoney + "\n";
				break;
			case '3':
				//System.out.println("                   3 登记支出");
				System.out.print("本次支出金额：");
				int money1 = Utility.readNumber();//获取用户输入的支出金额
				System.out.print("本次支出说明：");
				String minusMoney = Utility.readString();//获取支出说明的字符串
				
				if (balance >= money1){
					balance -= money1;
					details += "支出\t" + balance + "\t\t" + money1 + "\t\t" + minusMoney + "\n";
				}else{
					System.out.println("支出金额超出账户余额，支出失败！");	
				}
				
				break;
			case '4':
				//System.out.println("                   4 退    出");
				//当用户选择4且输入Y时，内部执行：isLoop = false
				System.out.println("\n确认是否退出（Y/N）：");
				char isExit = Utility.readConfirmSelection();//获取Utility类里面方法读取用户输入的数据
				if (isExit == 'Y'){
					isLoop = false;//若执行则不会再次进入while循环
				}
				break;//否则退出switch-case结构，但会继续进入循环
		
			}
		}
	}
}
