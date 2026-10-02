const { mdToPdf } = require('md-to-pdf');

(async () => {
    try {
        const pdf = await mdToPdf({ path: 'marabunta_valuation.md' }, {
            dest: 'marabunta-complete-sale.pdf',
            launch_options: {
                args: ['--no-sandbox', '--disable-setuid-sandbox']
            }
        });
        console.log('PDF Generated Successfully.');
    } catch (err) {
        console.error(err);
    }
})();