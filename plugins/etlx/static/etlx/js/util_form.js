function adjustIndices(removedIndex) {
    var $forms = $('.subform');
    $forms.each(function(i) {
        var $form = $(this);
        var index = parseInt($form.data('index'));
        var newIndex = index - 1;
        if (index < removedIndex) {
            // Skip
            return true;
        }
        // Change ID in form itself
        $form.attr('id', $form.attr('id').replace(index, newIndex));
        $form.data('index', newIndex);
        // Change IDs in form inputs
        $form.find('input').each(function(j) {
            var $item = $(this);
            $item.attr('id', $item.attr('id').replace(index, newIndex));
            $item.attr('name', $item.attr('name').replace(index, newIndex));
        });
    });
}
/**
 * Remove a form.
 */
function removeForm() {
    var $removedForm = $(this).closest('.subform');
    var removedIndex = parseInt($removedForm.data('index'));
    $removedForm.remove();
    // Update indices
    adjustIndices(removedIndex);
}
/**
 * Add a new form.
 */
function addForm() {
    var $templateForm = $('#lap-_-form');
    if (!$templateForm) {
        console.log('[ERROR] Cannot find template');
        return;
    }
    // Get Last index
    var $lastForm = $('.subform').last();
    var newIndex = 0;
    if ($lastForm.length > 0) {
        newIndex = parseInt($lastForm.data('index')) + 1;
    }
    // Maximum of 20 subforms
    if (newIndex > 20) {
        console.log('[WARNING] Reached maximum number of elements');
        return;
    }
    // Add elements
    var $newForm = $templateForm.clone();
    $newForm.attr('id', $newForm.attr('id').replace('_', newIndex));
    $newForm.data('index', newIndex);
    $newForm.find('input').each(function(idx) {
        var $item = $(this);
        $item.attr('id', $item.attr('id').replace('_', newIndex));
        $item.attr('name', $item.attr('name').replace('_', newIndex));
    });
    // Append
    $('#subforms-container').append($newForm);
    $newForm.addClass('subform');
    $newForm.removeClass('is-hidden');
    $newForm.find('.remove').click(removeForm);
}